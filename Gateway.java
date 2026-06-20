import java.io.*;
import java.net.*;
import java.util.UUID;
import java.util.concurrent.*;

public class Gateway {

    private static final ExecutorService pool = Executors.newFixedThreadPool(100);
    private static boolean esBackup = false;
    private static volatile boolean primarioActivo = true;

    public static void main(String[] args) {
        esBackup = args.length > 0 && args[0].equalsIgnoreCase("backup");
        int puerto = esBackup ? Config.PUERTO_GATEWAY_BACKUP : Config.PUERTO_GATEWAY;

        if (esBackup) {
            monitorearPrimario();
        }

        try (ServerSocket serverSocket = new ServerSocket(puerto)) {
            System.out.println("Gateway " + (esBackup ? "BACKUP" : "PRIMARIO")
                    + " corriendo en el puerto " + puerto);

            while (true) {
                Socket socketCliente = serverSocket.accept();
                pool.execute(new ManejarCliente(socketCliente));
            }
        } catch (IOException e) {
            System.out.println("Error en Gateway: " + e.getMessage());
        }
    }

    private static void monitorearPrimario() {
        Thread hilo = new Thread(() -> {
            while (true) {
                try (Socket s = new Socket(Config.HOST_GATEWAY_PRIMARIO, Config.PUERTO_GATEWAY);
                     ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {
                    while (in.readObject() != null) { }
                } catch (Exception e) {
                    System.out.println("Gateway primario no disponible. Backup asume el control.");
                    primarioActivo = false;
                }
                if (!primarioActivo) break;
                try { Thread.sleep(Config.TIEMPO_RECONEXION_MS); } catch (Exception ex) { break; }
            }
        });
        hilo.setDaemon(true);
        hilo.start();
    }

    // ─────────────────── SELECCION POR PING ───────────────────

    private static String seleccionarServidorPrincipal() {
        String mejor = null;
        long mejorTiempo = Long.MAX_VALUE;

        System.out.println("--- Sondeo de servidores principales ---");
        for (String[] srv : Config.SERVIDORES_PRINCIPALES) {
            String host = srv[0];
            int puerto = Integer.parseInt(srv[1]);
            String nombre = srv[2];

            try {
                long inicio = System.nanoTime();
                Socket s = new Socket();
                s.connect(new InetSocketAddress(host, puerto), (int) Config.TIMEOUT_PING_MS);
                long tiempoMs = (System.nanoTime() - inicio) / 1_000_000;
                s.close();

                System.out.println("  " + nombre + " (" + host + ":" + puerto + ") " + tiempoMs + "ms");
                if (tiempoMs < mejorTiempo) {
                    mejorTiempo = tiempoMs;
                    mejor = host + ":" + puerto;
                }
            } catch (Exception e) {
                System.out.println("  " + nombre + " (" + host + ":" + puerto + ") ---");
            }
        }

        if (mejor == null) {
            System.out.println("  Ningun servidor disponible, usando default.");
            mejor = Config.HOST_PRINCIPAL_PRIMARIO + ":" + Config.PUERTO_PRINCIPAL_PRIMARIO;
        } else {
            System.out.println("  >> Seleccionado: " + mejor + " (" + mejorTiempo + "ms)");
        }
        return mejor;
    }

    // ─────────────────── MANEJO DE CLIENTES ───────────────────

    private static class ManejarCliente implements Runnable {
        private Socket socket;

        public ManejarCliente(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                PaqueteDatos paqueteEntrada = (PaqueteDatos) in.readObject();

                if (!"AUTH".equals(paqueteEntrada.getTipo())) {
                    socket.close();
                    return;
                }

                String usuario = paqueteEntrada.getEmisor();
                String sessionId = UUID.randomUUID().toString().substring(0, 8);
                String dirPrincipal = seleccionarServidorPrincipal();

                PaqueteDatos authOk = new PaqueteDatos("AUTH_OK", "Gateway", usuario, null);
                authOk.setSessionId(sessionId);
                authOk.setServidorDestino(dirPrincipal);
                out.writeObject(authOk);
                out.flush();

                System.out.println("Usuario autenticado: " + usuario + " -> redirigido a " + dirPrincipal);

                try {
                    while (in.readObject() != null) { }
                } catch (Exception e) { }

            } catch (Exception e) {
                System.out.println("Error en Gateway: " + e.getMessage());
            } finally {
                try { socket.close(); } catch (IOException e) { }
            }
        }
    }
}
