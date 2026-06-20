import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class LoadGenerator {

    static final String[][] GATEWAYS = {
        { Config.HOST_GATEWAY_PRIMARIO, String.valueOf(Config.PUERTO_GATEWAY) },
        { Config.HOST_GATEWAY_BACKUP, String.valueOf(Config.PUERTO_GATEWAY_BACKUP) }
    };
    static final String[][] PRINCIPALES = {
        { Config.HOST_PRINCIPAL_PRIMARIO, String.valueOf(Config.PUERTO_PRINCIPAL_PRIMARIO) },
        { Config.HOST_PRINCIPAL_BACKUP, String.valueOf(Config.PUERTO_PRINCIPAL_BACKUP) },
        { "localhost", "5202" }
    };

    static final int NUM_CLIENTES = 50;
    static final int DURACION_SEG = 60;
    static final int FALLA_SEG = 30;

    static final MetricsCollector metrics = new MetricsCollector();
    static volatile int coordinatorPort = -1;
    static volatile boolean testRunning = true;
    static final CountDownLatch testDone = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        int numClientes = args.length > 0 ? Integer.parseInt(args[0]) : NUM_CLIENTES;
        int duracionSeg = args.length > 1 ? Integer.parseInt(args[1]) : DURACION_SEG;
        int fallaSeg = args.length > 2 ? Integer.parseInt(args[2]) : FALLA_SEG;

        System.out.println("=== GENERADOR DE CARGA ===");
        System.out.printf("Clientes: %d | Duracion: %ds | Falla inducida: t=%ds%n%n",
                numClientes, duracionSeg, fallaSeg);

        MetricsCollector.setDuracion(duracionSeg);

        List<VirtualClient> clientes = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numClientes; i++) {
            VirtualClient vc = new VirtualClient(i);
            clientes.add(vc);
            new Thread(vc, "VC-" + i).start();
            Thread.sleep(50);
        }

        Thread.sleep(3000);

        MonitorClient monitor = new MonitorClient();
        Thread monitorThread = new Thread(monitor, "Monitor");
        monitorThread.start();

        if (fallaSeg > 0) {
            final int fallaEnSeg = fallaSeg;
            new Thread(() -> {
                try {
                    Thread.sleep(fallaEnSeg * 1000L);
                    if (!testRunning) return;
                    injectFailure();
                } catch (InterruptedException e) {
                }
            }, "FailureInjector").start();
        }

        Thread.sleep(duracionSeg * 1000L);
        testRunning = false;
        metrics.printReportIntermedio();
        monitor.detener();
        monitorThread.join(5000);

        for (VirtualClient vc : clientes) {
            vc.detener();
        }

        System.out.println("\nRecolectando log de coordinacion...");
        int coordCount = monitor.obtenerConteoCoordinacionFinal();
        if (coordCount > 0) metrics.setCoordMsgs(coordCount);

        metrics.printReport(numClientes, duracionSeg, fallaSeg);
        testDone.countDown();
    }

    static void injectFailure() {
        int port = coordinatorPort;
        if (port <= 0) {
            System.out.println("\n*** No se detecto puerto de coordinador, no se puede inducir falla ***\n");
            return;
        }
        System.out.printf("%n*** INDUCIENDO FALLA: matando coordinador en puerto %d ***%n%n", port);
        metrics.marcarInicioFalla();
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"netstat", "-ano"});
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.contains(":" + port + " ") && line.contains("LISTENING")) {
                    String[] tokens = line.split("\\s+");
                    String pid = tokens[tokens.length - 1];
                    if (pid.matches("\\d+")) {
                        Runtime.getRuntime().exec(new String[]{"taskkill", "/F", "/PID", pid});
                        System.out.println("Coordinador eliminado (PID " + pid + " en puerto " + port + ")");
                        return;
                    }
                }
            }
            System.out.println("No se encontro proceso escuchando en puerto " + port);
        } catch (Exception e) {
            System.out.println("Error inyectando falla: " + e.getMessage());
        }
    }

    // ─────────────────── VIRTUAL CLIENT ───────────────────────────

    static class VirtualClient implements Runnable {
        final int id;
        final String username;
        volatile boolean running = true;
        Socket socket;
        ObjectOutputStream out;
        ObjectInputStream in;
        String sessionId;
        boolean conectado = false;

        VirtualClient(int id) {
            this.id = id;
            this.username = "load_" + id;
        }

        void detener() {
            running = false;
            try { if (socket != null) socket.close(); } catch (Exception e) {}
        }

        @Override
        public void run() {
            while (running && testRunning) {
                try {
                    conectarGateway();
                    conectarPrincipal();
                    if (!conectado) {
                        Thread.sleep(500);
                        continue;
                    }
                    ejecutarPrueba();
                } catch (Exception e) {
                    if (running) {
                        metrics.contarError();
                    }
                }
                cerrarConexion();
            }
        }

        void conectarGateway() throws Exception {
            Socket gwSocket = null;
            ObjectInputStream gwIn = null;
            ObjectOutputStream gwOut = null;
            sessionId = null;

            for (int i = 0; i < GATEWAYS.length && sessionId == null; i++) {
                String host = GATEWAYS[i][0];
                int port = Integer.parseInt(GATEWAYS[i][1]);
                try {
                    gwSocket = new Socket(host, port);
                    gwSocket.setSoTimeout(2000);
                    gwOut = new ObjectOutputStream(gwSocket.getOutputStream());
                    gwOut.flush();
                    gwIn = new ObjectInputStream(gwSocket.getInputStream());

                    PaqueteDatos auth = new PaqueteDatos("AUTH", username, "", null);
                    gwOut.writeObject(auth);
                    gwOut.flush();

                    PaqueteDatos resp = (PaqueteDatos) gwIn.readObject();
                    if ("AUTH_OK".equals(resp.getTipo())) {
                        sessionId = resp.getSessionId();
                        String dirDestino = resp.getServidorDestino();
                        if (dirDestino != null && dirDestino.contains(":")) {
                            String[] parts = dirDestino.split(":");
                            metrics.destinoSP = parts[0] + ":" + parts[1];
                        }
                    }
                    gwSocket.close();
                } catch (Exception e) {
                    if (gwSocket != null) try { gwSocket.close(); } catch (Exception e2) {}
                }
            }

            if (sessionId == null) {
                throw new RuntimeException("No se pudo autenticar");
            }
        }

        void conectarPrincipal() throws Exception {
            int intento = 0;
            while (running && testRunning && sessionId != null) {
                int idx = intento % PRINCIPALES.length;
                String host = PRINCIPALES[idx][0];
                int port = Integer.parseInt(PRINCIPALES[idx][1]);
                intento++;

                Socket s = null;
                ObjectOutputStream o = null;
                ObjectInputStream i = null;
                try {
                    s = new Socket(host, port);
                    s.setSoTimeout(5000);
                    o = new ObjectOutputStream(s.getOutputStream());
                    o.flush();
                    i = new ObjectInputStream(s.getInputStream());

                    PaqueteDatos connect = new PaqueteDatos("CONNECT", username, "", null);
                    connect.setSessionId(sessionId);
                    o.writeObject(connect);
                    o.flush();

                    PaqueteDatos resp = (PaqueteDatos) i.readObject();
                    if ("NO_COORDINATOR".equals(resp.getTipo()) || "NOT_LEADER".equals(resp.getTipo())) {
                        String coorPort = resp.getMensaje();
                        s.close();
                        for (int k = 0; k < PRINCIPALES.length; k++) {
                            if (PRINCIPALES[k][1].equals(coorPort)) {
                                intento = k;
                                break;
                            }
                        }
                        continue;
                    }
                    if (!"CONNECT_OK".equals(resp.getTipo())) {
                        s.close();
                        continue;
                    }

                    if (coordinatorPort <= 0) {
                        coordinatorPort = port;
                    }

                    this.socket = s;
                    this.out = o;
                    this.in = i;
                    this.conectado = true;
                    metrics.registrarReconexion();
                    return;

                } catch (Exception e) {
                    if (s != null) try { s.close(); } catch (Exception e2) {}
                    metrics.contarError();
                    Thread.sleep(100);
                }
            }
        }

        void ejecutarPrueba() throws Exception {
            Thread receiver = new Thread(() -> {
                try {
                    PaqueteDatos p;
                    while (running && conectado) {
                        p = (PaqueteDatos) in.readObject();
                        if ("CHAT".equals(p.getTipo())) {
                            String msg = p.getMensaje();
                            if (msg != null && msg.startsWith("__LOAD_" + id + "_")) {
                                String seqId = msg.substring(7);
                                metrics.registrarLatencia(seqId);
                            }
                        } else if ("CONNECT_OK".equals(p.getTipo())) {
                            metrics.registrarReconexion();
                        }
                    }
                } catch (Exception e) {
                    if (running) conectado = false;
                }
            }, "RCV-" + id);
            receiver.start();

            int seq = 0;
            while (running && testRunning && conectado) {
                try {
                    String msgId = id + "_" + seq;
                    String texto = "__LOAD_" + msgId;
                    PaqueteDatos chat = new PaqueteDatos("CHAT", username, texto, null);
                    chat.setSessionId(sessionId);

                    metrics.registrarEnvio(msgId);
                    out.writeObject(chat);
                    out.flush();
                    metrics.contarRequest();

                    seq++;
                    Thread.sleep(100 + (id % 5) * 10);
                } catch (IOException e) {
                    metrics.contarError();
                    conectado = false;
                    break;
                }
            }

            receiver.interrupt();
        }

        void cerrarConexion() {
            conectado = false;
            try { if (socket != null) socket.close(); } catch (Exception e) {}
            socket = null;
            out = null;
            in = null;
        }
    }

    // ─────────────────── METRICS COLLECTOR ────────────────────────

    static class MetricsCollector {
        final AtomicLong totalRequests = new AtomicLong(0);
        final AtomicLong totalErrors = new AtomicLong(0);
        final ConcurrentHashMap<String, Long> pendingLatency = new ConcurrentHashMap<>();
        final ConcurrentLinkedQueue<Long> latencias = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<Long> latenciasDuranteFalla = new ConcurrentLinkedQueue<>();
        final long[] throughputPorSegundo;
        final AtomicLong requestCountThisSecond = new AtomicLong(0);
        volatile int segundoActual = 0;
        volatile long startTimeNanos;
        volatile long failureStartMs = 0;
        volatile long recoveryEndMs = 0;
        volatile long lastReconnectMs = 0;
        volatile String destinoSP = "";
        int coordMsgs = 0;

        MetricsCollector() {
            throughputPorSegundo = new long[120];
            startTimeNanos = System.nanoTime();
            Thread recorder = new Thread(() -> {
                while (testRunning) {
                    try {
                        Thread.sleep(1000);
                        int seg = (int) ((System.nanoTime() - startTimeNanos) / 1_000_000_000);
                        if (seg < throughputPorSegundo.length) {
                            throughputPorSegundo[seg] = requestCountThisSecond.getAndSet(0);
                        } else {
                            requestCountThisSecond.set(0);
                        }
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }, "ThroughputRecorder");
            recorder.setDaemon(true);
            recorder.start();
        }

        static void setDuracion(int seg) {}

        void contarRequest() {
            totalRequests.incrementAndGet();
            requestCountThisSecond.incrementAndGet();
        }

        void contarError() {
            totalErrors.incrementAndGet();
        }

        void registrarEnvio(String msgId) {
            pendingLatency.put(msgId, System.nanoTime());
        }

        void registrarLatencia(String seqId) {
            Long start = pendingLatency.remove(seqId);
            if (start != null) {
                long ms = (System.nanoTime() - start) / 1_000_000;
                latencias.add(ms);
                if (estaEnFalla()) {
                    latenciasDuranteFalla.add(ms);
                }
            }
        }

        void marcarInicioFalla() {
            failureStartMs = System.currentTimeMillis();
        }

        boolean estaEnFalla() {
            long now = System.currentTimeMillis();
            return failureStartMs > 0 && (recoveryEndMs == 0 || now - failureStartMs < 10000);
        }

        void registrarReconexion() {
            long now = System.currentTimeMillis();
            if (failureStartMs > 0 && recoveryEndMs == 0) {
                recoveryEndMs = now;
            }
            lastReconnectMs = now;
        }

        void setCoordMsgs(int n) {
            coordMsgs = n;
        }

        long getRecoveryTimeMs() {
            if (failureStartMs > 0 && recoveryEndMs > 0) {
                return recoveryEndMs - failureStartMs;
            }
            return -1;
        }

        void printReportIntermedio() {
            long totalReq = totalRequests.get();
            long totalErr = totalErrors.get();
            double pctError = totalReq > 0 ? (totalErr * 100.0 / totalReq) : 0;
            long recuperacion = getRecoveryTimeMs();
            System.out.printf("%n=== CORTE: Fin de ventana de carga ===%n");
            System.out.printf("Requests: %d | Errores: %d (%.2f%%)%n", totalReq, totalErr, pctError);
            if (recuperacion > 0) {
                System.out.printf("Tiempo recuperacion: %d ms%n", recuperacion);
            }
        }

        void printReport(int numClientes, int duracionSeg, int fallaSeg) {
            long totalReq = totalRequests.get();
            long totalErr = totalErrors.get();
            double throughput = duracionSeg > 0 ? (double) totalReq / duracionSeg : 0;
            double pctError = totalReq > 0 ? (totalErr * 100.0 / totalReq) : 0;

            long[] latArr = latencias.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(latArr);
            double avgLat = 0;
            double p95Lat = 0;
            if (latArr.length > 0) {
                avgLat = Arrays.stream(latArr).average().orElse(0);
                int p95Idx = (int) Math.ceil(latArr.length * 0.95) - 1;
                if (p95Idx < 0) p95Idx = 0;
                if (p95Idx >= latArr.length) p95Idx = latArr.length - 1;
                p95Lat = latArr[p95Idx];
            }

            long[] latFallaArr = latenciasDuranteFalla.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(latFallaArr);
            double maxLatFalla = latFallaArr.length > 0 ? latFallaArr[latFallaArr.length - 1] : 0;

            long recuperacion = getRecoveryTimeMs();

            System.out.println();
            System.out.println("╔══════════════════════════════════════════════════╗");
            System.out.println("║           RESULTADOS PRUEBA DE CARGA           ║");
            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.printf("║ %-50s ║%n", "Clientes: " + numClientes + " | Duracion: " + duracionSeg + "s | Falla: t=" + fallaSeg + "s");
            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.printf("║ %-30s %17s ║%n", "Throughput promedio", String.format("%.1f msg/s", throughput));
            System.out.printf("║ %-30s %17s ║%n", "Latencia promedio", String.format("%.1f ms", avgLat));
            System.out.printf("║ %-30s %17s ║%n", "Latencia P95", String.format("%.1f ms", p95Lat));
            System.out.printf("║ %-30s %17s ║%n", "Tasa de error", String.format("%.2f%%", pctError));
            System.out.printf("║ %-30s %17s ║%n", "Mensajes coordinacion", coordMsgs > 0 ? String.valueOf(coordMsgs) : "N/A");
            System.out.println("╠══════════════════════════════════════════════════╣");
            System.out.println("║         METRICAS DURANTE FALLA INDUCIDA        ║");
            System.out.println("╠══════════════════════════════════════════════════╣");
            if (recuperacion > 0) {
                System.out.printf("║ %-30s %17s ║%n", "Tiempo recuperacion", recuperacion + " ms");
            } else {
                System.out.printf("║ %-30s %17s ║%n", "Tiempo recuperacion", "No detectada");
            }
            System.out.printf("║ %-30s %17s ║%n", "Latencia maxima en falla", String.format("%.1f ms", maxLatFalla));
            System.out.println("╚══════════════════════════════════════════════════╝");
            System.out.println();

            System.out.println("Throughput por segundo:");
            System.out.printf("%-6s %-12s %s%n", "seg", "msg/s", "barra");
            System.out.println("------------------------------------------");
            for (int i = 0; i <= duracionSeg && i < throughputPorSegundo.length; i++) {
                long tps = throughputPorSegundo[i];
                int barLen = (int) Math.min(tps / 5, 40);
                StringBuilder bar = new StringBuilder();
                for (int b = 0; b < barLen; b++) bar.append('#');
                System.out.printf("%-6d %-12s %s%n", i, tps, bar.toString());
            }
        }
    }

    // ─────────────────── MONITOR CLIENT ───────────────────────────

    static class MonitorClient implements Runnable {
        volatile boolean running = true;
        final ConcurrentLinkedQueue<String> logSnapshots = new ConcurrentLinkedQueue<>();
        Socket socket;
        ObjectOutputStream out;
        ObjectInputStream in;
        String sessionId;

        void detener() {
            running = false;
            try { if (socket != null) socket.close(); } catch (Exception e) {}
        }

        int obtenerConteoCoordinacionFinal() {
            if (coordinatorPort <= 0) return 0;
            try {
                String log = obtenerLog(coordinatorPort);
                if (log != null) {
                    return contarEventosCoordinacion(log);
                }
            } catch (Exception e) {}
            return 0;
        }

        String obtenerLog(int port) throws Exception {
            Socket s = null;
            ObjectOutputStream o = null;
            ObjectInputStream i = null;
            String sid = null;
            try {
                s = new Socket("localhost", port);
                s.setSoTimeout(3000);
                o = new ObjectOutputStream(s.getOutputStream());
                o.flush();
                i = new ObjectInputStream(s.getInputStream());

                PaqueteDatos auth = new PaqueteDatos("AUTH", "monitor", "", null);
                o.writeObject(auth);
                o.flush();
                PaqueteDatos authResp = (PaqueteDatos) i.readObject();
                sid = authResp.getSessionId();

                PaqueteDatos connect = new PaqueteDatos("CONNECT", "monitor", "", null);
                connect.setSessionId(sid);
                o.writeObject(connect);
                o.flush();

                PaqueteDatos connResp = (PaqueteDatos) i.readObject();
                if ("NO_COORDINATOR".equals(connResp.getTipo()) || "NOT_LEADER".equals(connResp.getTipo())) {
                    s.close();
                    int newPort = Integer.parseInt(connResp.getMensaje());
                    return obtenerLog(newPort);
                }

                PaqueteDatos p;
                while ((p = (PaqueteDatos) i.readObject()) != null) {
                    if ("USER_LIST".equals(p.getTipo())) break;
                }

                o.writeObject(new PaqueteDatos("LOG_REQUEST", "monitor", "", null));
                o.flush();

                PaqueteDatos logResp;
                while ((logResp = (PaqueteDatos) i.readObject()) != null) {
                    if ("LOG_RESPONSE".equals(logResp.getTipo())) {
                        String log = logResp.getMensaje();
                        s.close();
                        return log;
                    }
                }
                s.close();
                return null;
            } catch (Exception e) {
                try { if (s != null) s.close(); } catch (Exception e2) {}
                return null;
            }
        }

        int contarEventosCoordinacion(String log) {
            int count = 0;
            String[] lines = log.split("\n");
            for (String line : lines) {
                if (line.contains("ENTRAR_SC") || line.contains("SALIR_SC")
                        || line.contains("PROPONER_RAFT") || line.contains("CONFIRMAR_RAFT")
                        || line.contains("APLICAR_RAFT")
                        || line.contains("SER_COORDINADOR") || line.contains("COORDINADOR_ACTUALIZADO")) {
                    count++;
                }
            }
            return count;
        }

        String obtenerLogActual() throws Exception {
            int port = coordinatorPort;
            if (port <= 0) return null;
            return obtenerLog(port);
        }

        @Override
        public void run() {
            while (running && testRunning) {
                try {
                    Thread.sleep(5000);
                    if (!running || !testRunning) break;
                    String log = obtenerLogActual();
                    if (log != null) {
                        logSnapshots.add(log);
                    }
                } catch (Exception e) {
                }
            }
        }
    }
}
