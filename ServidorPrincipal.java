import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class ServidorPrincipal {

    private static final ExecutorService pool = Executors.newFixedThreadPool(100);

    // Clientes conectados a este ServidorPrincipal
    private static List<ObjectOutputStream> flujoSalidaClientes =
            Collections.synchronizedList(new ArrayList<>());
    private static Map<String, String> sesiones = new ConcurrentHashMap<>();

    // Replicacion
    private static List<ObjectOutputStream> flujoSalidaReplicas =
            Collections.synchronizedList(new ArrayList<>());
    private static List<PaqueteDatos> bufferHistorial =
            Collections.synchronizedList(new ArrayList<>());

    // Configuracion local
    private static int idNodo;
    private static int delayMs = 0;

    // Reloj de Lamport y Log de eventos
    private static int relojLamport = 0;
    private static LogEvento logEventos = new LogEvento();

    // ─── BULLY ──────────────────────────────────────────────────────
    private static volatile int coordinadorId;
    private static volatile boolean soyCoordinador = false;
    private static volatile boolean eleccionEnProgreso = false;

    // ─── RICART Y AGRAWALA ──────────────────────────────────────────
    private static volatile boolean quieroSC = false;
    private static volatile boolean enSC = false;
    private static int miLamportSolicitud = 0;
    private static Set<String> colaDiferidos = ConcurrentHashMap.newKeySet();
    private static Set<String> respuestasPendientes = ConcurrentHashMap.newKeySet();

    // ─── RAFT ────────────────────────────────────────────────────────
    private static List<RaftLogEntry> raftLog =
            Collections.synchronizedList(new ArrayList<>());
    private static volatile int committedRaftIndex = -1;
    private static volatile int lastAppliedRaftIndex = -1;
    private static volatile int currentRaftTerm = 0;
    private static Set<String> raftAcksPendientes = ConcurrentHashMap.newKeySet();
    private static Object raftLock = new Object();
    private static volatile ScheduledExecutorService raftHeartbeatTimer;

    // Conexion a ServidorChat
    private static Socket socketChat;
    private static ObjectOutputStream chatOut;
    private static volatile boolean chatConectado = false;

    public static void main(String[] args) {
        // Parsear argumentos: "backup" -> 5201, delay numerico -> 5202
        boolean esBackup = false;
        for (String arg : args) {
            if ("backup".equalsIgnoreCase(arg)) esBackup = true;
            else if (arg.matches("\\d+")) delayMs = Integer.parseInt(arg);
        }

        if (delayMs > 0) {
            idNodo = 5202; // lejano con delay
        } else if (esBackup) {
            idNodo = 5201;
        } else {
            idNodo = 5200;
        }

        // Determinar quien es el coordinador inicial
        coordinadorId = idNodo; // cada nodo asume que el mismo es coordinador inicial
        // En realidad el primer coordinador deberia ser el de mayor ID vivo
        // Hacemos una eleccion rapida al inicio
        soyCoordinador = false;

        System.out.println("ServidorPrincipal ID=" + idNodo
                + " iniciando (delay=" + delayMs + "ms)");

        conectarServidorChat();

        try (ServerSocket serverSocket = new ServerSocket(idNodo)) {
            String extra = delayMs > 0 ? " (delay: " + delayMs + "ms)" : "";
            System.out.println("ServidorPrincipal " + idNodo
                    + " corriendo en el puerto " + idNodo + extra);

            // Eleccion inicial (todos participan)
            iniciarEleccion();

            // Despues de la eleccion, los no-coordinadores se conectan al coordinador
            if (!soyCoordinador) {
                conectarCoordinador();
            }

            System.out.println("Rol final: "
                    + (soyCoordinador ? "COORDINADOR" : "OBSERVADOR"));

            while (true) {
                Socket socketCliente = serverSocket.accept();
                pool.execute(new ManejarCliente(socketCliente));
            }
        } catch (IOException e) {
            System.out.println("Error en ServidorPrincipal: " + e.getMessage());
        }
    }

    // ─────────────────── BULLY ──────────────────────────────────────

    private static int[] obtenerIdsPares() {
        List<Integer> ids = new ArrayList<>();
        for (String[] srv : Config.SERVIDORES_PRINCIPALES) {
            int id = Integer.parseInt(srv[1]);
            if (id != idNodo) ids.add(id);
        }
        return ids.stream().mapToInt(i -> i).toArray();
    }

    private static int[] obtenerIdsSuperiores() {
        List<Integer> ids = new ArrayList<>();
        for (String[] srv : Config.SERVIDORES_PRINCIPALES) {
            int id = Integer.parseInt(srv[1]);
            if (id > idNodo) ids.add(id);
        }
        return ids.stream().mapToInt(i -> i).toArray();
    }

    private static PaqueteDatos enviarMensajeSP(int idDestino, PaqueteDatos msg, boolean esperarRespuesta) {
        return enviarMensajeSP(idDestino, msg, esperarRespuesta, Config.TIMEOUT_ELECCION_MS);
    }

    private static PaqueteDatos enviarMensajeSP(int idDestino, PaqueteDatos msg,
            boolean esperarRespuesta, long timeoutMs) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", idDestino), (int) timeoutMs);
            s.setSoTimeout((int) timeoutMs);
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            out.flush();
            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            out.writeObject(msg);
            out.flush();
            if (esperarRespuesta) {
                return (PaqueteDatos) in.readObject();
            }
        } catch (Exception e) {
            // nodo caido o timeout
        }
        return null;
    }

    private static synchronized void iniciarEleccion() {
        if (eleccionEnProgreso) return;
        eleccionEnProgreso = true;

        int[] superiores = obtenerIdsSuperiores();
        System.out.println("Eleccion iniciada por " + idNodo
                + ". IDs superiores: " + Arrays.toString(superiores));

        boolean alguienRespondio = false;

        for (int idSup : superiores) {
            PaqueteDatos req = new PaqueteDatos("ELECTION", String.valueOf(idNodo), "", null);
            PaqueteDatos resp = enviarMensajeSP(idSup, req, true);
            if (resp != null && "OK".equals(resp.getTipo())) {
                System.out.println(idSup + " respondio OK. Esperando su coordinacion.");
                alguienRespondio = true;
                break;
            }
        }

        if (!alguienRespondio) {
            System.out.println("Nadie respondio. " + idNodo + " se declara coordinador.");
            declararCoordinador();
        }

        eleccionEnProgreso = false;
    }

    private static void declararCoordinador() {
        soyCoordinador = true;
        coordinadorId = idNodo;

        relojLamport++;
        logEventos.registrar(relojLamport, "ServidorPrincipal" + idNodo,
                "SER_COORDINADOR", "ID=" + idNodo);

        iniciarHeartbeatRaft();

        // Notificar a todos los peers
        PaqueteDatos msg = new PaqueteDatos("COORDINATOR", String.valueOf(idNodo),
                String.valueOf(idNodo), null);
        for (int idPeer : obtenerIdsPares()) {
            enviarMensajeSP(idPeer, msg, false);
        }

        System.out.println("ServidorPrincipal " + idNodo + " ahora es COORDINADOR.");
    }

    // ─────────────────── RICART Y AGRAWALA ──────────────────────────

    private static Object lockRicart = new Object();

    private static void solicitarSC() {
        synchronized (lockRicart) {
            while (quieroSC) {
                try { lockRicart.wait(); } catch (InterruptedException e) { return; }
            }
            quieroSC = true;

            relojLamport++;
            miLamportSolicitud = relojLamport;

            respuestasPendientes.clear();
            for (int id : obtenerIdsPares()) {
                respuestasPendientes.add(String.valueOf(id));
            }

            PaqueteDatos req = new PaqueteDatos("RICART_REQUEST",
                    String.valueOf(idNodo), "", null);
            req.setRelojLamport(miLamportSolicitud);

            for (int id : obtenerIdsPares()) {
                final int idPeer = id;
                pool.execute(() -> {
                    PaqueteDatos resp = enviarMensajeSP(idPeer, req, true, Config.TIMEOUT_RICART_MS);
                    if (resp != null && "RICART_OK".equals(resp.getTipo())) {
                        recibirOK(String.valueOf(idPeer));
                    } else {
                        // Timeout o peer caido → considerar OK
                        recibirOK(String.valueOf(idPeer));
                    }
                });
            }

            while (!respuestasPendientes.isEmpty()) {
                try { lockRicart.wait(); } catch (InterruptedException e) { return; }
            }

            enSC = true;
            logEventos.registrar(relojLamport, "ServidorPrincipal" + idNodo,
                    "ENTRAR_SC", "Lamport=" + miLamportSolicitud);
        }
    }

    private static void liberarSC() {
        synchronized (lockRicart) {
            enSC = false;
            quieroSC = false;

            logEventos.registrar(relojLamport, "ServidorPrincipal" + idNodo,
                    "SALIR_SC", "");

            // Enviar OK a los diferidos
            for (String deferido : colaDiferidos) {
                PaqueteDatos ok = new PaqueteDatos("RICART_OK",
                        String.valueOf(idNodo), "", null);
                enviarMensajeSP(Integer.parseInt(deferido), ok, false);
            }
            colaDiferidos.clear();

            lockRicart.notifyAll();
        }
    }

    private static void recibirOK(String emisorId) {
        synchronized (lockRicart) {
            respuestasPendientes.remove(emisorId);
            if (respuestasPendientes.isEmpty()) {
                lockRicart.notifyAll();
            }
        }
    }

    // ─────────────────── CONEXION A SERVIDORCHAT ────────────────────

    private static void conectarServidorChat() {
        Thread hilo = new Thread(() -> {
            while (true) {
                if (chatConectado) {
                    try { Thread.sleep(2000); } catch (Exception ex) { break; }
                    continue;
                }

                int[] puertos = { Config.PUERTO_TEXTO_PRIMARIO, Config.PUERTO_TEXTO_BACKUP };

                for (int puertoChat : puertos) {
                    try {
                        socketChat = new Socket(Config.HOST_TEXTO_PRIMARIO, puertoChat);
                        chatOut = new ObjectOutputStream(socketChat.getOutputStream());
                        chatOut.flush();
                        ObjectInputStream chatIn = new ObjectInputStream(socketChat.getInputStream());
                        chatConectado = true;
                        System.out.println("ServidorPrincipal conectado a ServidorChat puerto " + puertoChat);

                        Thread escuchaChat = new Thread(() -> {
                            try {
                                PaqueteDatos p;
                                while ((p = (PaqueteDatos) chatIn.readObject()) != null) {
                                    if ("CHAT".equals(p.getTipo())) {
                                        if (p.getRelojLamport() > relojLamport)
                                            relojLamport = p.getRelojLamport();
                                        relojLamport++;
                                        p.setRelojLamport(relojLamport);
                                        logEventos.registrar(relojLamport,
                                            "ServidorPrincipal" + idNodo, "DIFUNDIR_CHAT",
                                            p.getEmisor() + ": " + p.getMensaje());
                                        bufferHistorial.add(p);
                                        if (bufferHistorial.size() > Config.MAX_HISTORIAL_CHAT) {
                                            bufferHistorial.remove(0);
                                        }
                                        difundirMensaje(p);
                                    }
                                }
                            } catch (Exception e) {
                                System.out.println("Conexion con ServidorChat perdida.");
                                chatConectado = false;
                            }
                        });
                        escuchaChat.setDaemon(true);
                        escuchaChat.start();

                        break;
                    } catch (IOException e) {
                        System.out.println("No se pudo conectar a ServidorChat puerto " + puertoChat);
                    }
                }

                if (!chatConectado) {
                    System.out.println("ServidorChat no disponible. Reintentando...");
                }

                try { Thread.sleep(Config.TIEMPO_RECONEXION_MS); } catch (Exception ex) { break; }
            }
        });
        hilo.setDaemon(true);
        hilo.start();
    }

    // ─────────────────── CONEXION AL COORDINADOR ────────────────────

    private static void conectarCoordinador() {
        Thread hilo = new Thread(() -> {
            while (!soyCoordinador) {
                int coorId = coordinadorId;
                if (coorId == idNodo) {
                    soyCoordinador = true;
                    break;
                }

                try (Socket s = new Socket("localhost", coorId);
                     ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

                    PaqueteDatos auth = new PaqueteDatos("AUTH", "PRINCIPAL_BACKUP", "", null);
                    out.writeObject(auth);
                    out.flush();
                    System.out.println("Observador " + idNodo
                            + " conectado al coordinador " + coorId);

                    PaqueteDatos p;
                    while ((p = (PaqueteDatos) in.readObject()) != null) {
                        switch (p.getTipo()) {
                            case "CHAT":
                                if (p.getRelojLamport() > relojLamport)
                                    relojLamport = p.getRelojLamport();
                                relojLamport++;
                                logEventos.registrar(relojLamport,
                                    "ServidorPrincipal" + idNodo + "Backup",
                                    "REPLICAR_CHAT",
                                    p.getEmisor() + ": " + p.getMensaje());
                                bufferHistorial.add(p);
                                if (bufferHistorial.size() > Config.MAX_HISTORIAL_CHAT) {
                                    bufferHistorial.remove(0);
                                }
                                break;
                            case "SESSION_SYNC":
                                String sid = p.getMensaje();
                                if (sid == null || sid.isEmpty()) {
                                    sesiones.remove(p.getEmisor());
                                } else {
                                    sesiones.put(sid, p.getEmisor());
                                }
                                break;
                            case "RAFT_LOG_SYNC":
                                RaftLogEntry[] entries = p.getRaftEntries();
                                if (entries != null) {
                                    for (RaftLogEntry entry : entries) {
                                        if (entry.index >= raftLog.size()) {
                                            raftLog.add(entry);
                                        }
                                    }
                                    System.out.println("Raft: log sincronizado con "
                                            + entries.length + " entradas del lider");
                                }
                                committedRaftIndex = p.getRaftLeaderCommit();
                                aplicarEntradasPendientes();
                                break;
                        }
                    }
                } catch (EOFException | SocketException e) {
                    System.out.println("Coordinador " + coordinadorId
                            + " no disponible. Iniciando eleccion...");
                    if (!soyCoordinador) {
                        iniciarEleccion();
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Error de conexion con coordinador: " + e.getMessage());
                    if (!soyCoordinador) {
                        iniciarEleccion();
                    }
                }

                try { Thread.sleep(Config.TIEMPO_RECONEXION_MS); } catch (Exception ex) { break; }
            }
        });
        hilo.setDaemon(true);
        hilo.start();
    }

    // ─────────────────── MANEJO DE CLIENTES ────────────────────────

    private static class ManejarCliente implements Runnable {
        private Socket socket;
        private ObjectOutputStream out;
        private ObjectInputStream in;
        private String sessionId;
        private String usuario;

        public ManejarCliente(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                if (delayMs > 0) {
                    try { Thread.sleep(delayMs); } catch (InterruptedException ie) { }
                }

                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());

                PaqueteDatos paqueteEntrada = (PaqueteDatos) in.readObject();
                String tipo = paqueteEntrada.getTipo();

                // ── RICART: recibir OK (puede llegar en cualquier conexion) ──
                if ("RICART_OK".equals(tipo)) {
                    recibirOK(paqueteEntrada.getEmisor());
                    socket.close();
                    return;
                }

                // ── ELECTION ──
                if ("ELECTION".equals(tipo)) {
                    int emisorId = Integer.parseInt(paqueteEntrada.getEmisor());
                    System.out.println("Recibido ELECTION de " + emisorId);

                    PaqueteDatos ok = new PaqueteDatos("OK", String.valueOf(idNodo), "", null);
                    out.writeObject(ok);
                    out.flush();

                    if (idNodo > emisorId) {
                        pool.execute(() -> iniciarEleccion());
                    }

                    socket.close();
                    return;
                }

                // ── COORDINATOR ──
                if ("COORDINATOR".equals(tipo)) {
                    int nuevoCoor = Integer.parseInt(paqueteEntrada.getMensaje());
                    System.out.println("Nuevo coordinador: " + nuevoCoor);
                    coordinadorId = nuevoCoor;

                    if (idNodo == nuevoCoor) {
                        soyCoordinador = true;
                        if (!chatConectado) conectarServidorChat();
                        iniciarHeartbeatRaft();
                    } else {
                        soyCoordinador = false;
                        detenerHeartbeatRaft();
                        conectarCoordinador();
                    }

                    relojLamport++;
                    logEventos.registrar(relojLamport,
                        "ServidorPrincipal" + idNodo, "COORDINADOR_ACTUALIZADO",
                        "coordinador=" + nuevoCoor);

                    socket.close();
                    return;
                }

                // ── RICART: recibir REQUEST ──
                if ("RICART_REQUEST".equals(tipo)) {
                    int reqId = Integer.parseInt(paqueteEntrada.getEmisor());
                    int reqLamport = paqueteEntrada.getRelojLamport();

                    synchronized (lockRicart) {
                        if (!quieroSC) {
                            // No interesado → OK inmediato
                            PaqueteDatos ok = new PaqueteDatos("RICART_OK",
                                    String.valueOf(idNodo), "", null);
                            out.writeObject(ok);
                            out.flush();
                        } else if (enSC) {
                            // En SC → diferir
                            colaDiferidos.add(paqueteEntrada.getEmisor());
                        } else {
                            // Esperando SC → comparar prioridad
                            if (reqLamport < miLamportSolicitud
                                    || (reqLamport == miLamportSolicitud && reqId < idNodo)) {
                                // Peticion entrante tiene mayor prioridad → diferir
                                colaDiferidos.add(paqueteEntrada.getEmisor());
                            } else {
                                // Yo tengo mayor prioridad → OK inmediato
                                PaqueteDatos ok = new PaqueteDatos("RICART_OK",
                                        String.valueOf(idNodo), "", null);
                                out.writeObject(ok);
                                out.flush();
                            }
                        }
                    }

                    socket.close();
                    return;
                }

                // ── RAFT: recibir AppendEntries (desde el lider) ──
                if ("RAFT_APPEND_ENTRIES".equals(tipo)) {
                    manejarAppendEntries(paqueteEntrada, out);
                    socket.close();
                    return;
                }

                // ── BACKUP (observador conectandose al coordinador) ──
                if ("AUTH".equals(tipo)
                        && "PRINCIPAL_BACKUP".equals(paqueteEntrada.getEmisor())) {
                    flujoSalidaReplicas.add(out);
                    System.out.println("Observador " + paqueteEntrada.getEmisor()
                            + " conectado para replicacion.");
                    enviarEstadoCompletoSesiones(out);
                    enviarRaftLogCompleto(out);
                    mantenerBackup();
                    return;
                }

                // ── CONNECT (cliente real) ──
                if (!"CONNECT".equals(tipo)) {
                    socket.close();
                    return;
                }

                // Si no soy coordinador, rechazo
                if (!soyCoordinador) {
                    PaqueteDatos rechazo = new PaqueteDatos("NO_COORDINATOR",
                            "ServidorPrincipal", String.valueOf(coordinadorId), null);
                    out.writeObject(rechazo);
                    out.flush();
                    socket.close();
                    return;
                }

                usuario = paqueteEntrada.getEmisor();
                sessionId = paqueteEntrada.getSessionId();
                if (sessionId == null || sessionId.isEmpty()) {
                    sessionId = UUID.randomUUID().toString().substring(0, 8);
                }
                sesiones.put(sessionId, usuario);
                flujoSalidaClientes.add(out);

                System.out.println("Cliente conectado a ServidorPrincipal " + idNodo
                        + ": " + usuario + " (session: " + sessionId + ")");

                PaqueteDatos confirmacion = new PaqueteDatos("CONNECT_OK",
                        "ServidorPrincipal",
                        "Conectado al servidor principal", null);
                confirmacion.setSessionId(sessionId);
                out.writeObject(confirmacion);
                out.flush();

                replicarSesion(sessionId, usuario, "ADD");
                difundirUSER_LIST();
                enviarHistorialCliente(out);

                while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {
                    switch (paqueteEntrada.getTipo()) {
                        case "CHAT":
                            synchronized (lockRicart) {
                                solicitarSC();
                                try {
                                    // Raft: consenso antes de aplicar
                                    proponerEntradaRaft(paqueteEntrada);

                                    relojLamport++;
                                    paqueteEntrada.setRelojLamport(relojLamport);
                                    logEventos.registrar(relojLamport,
                                        "ServidorPrincipal" + idNodo, "RECIBIR_CHAT",
                                        paqueteEntrada.getEmisor()
                                        + ": " + paqueteEntrada.getMensaje());

                                    if (chatConectado) {
                                        chatOut.writeObject(paqueteEntrada);
                                        chatOut.flush();
                                    } else {
                                        paqueteEntrada.setTimestamp(System.currentTimeMillis());
                                        bufferHistorial.add(paqueteEntrada);
                                        if (bufferHistorial.size() > Config.MAX_HISTORIAL_CHAT) {
                                            bufferHistorial.remove(0);
                                        }
                                        difundirMensaje(paqueteEntrada);
                                    }
                                } finally {
                                    liberarSC();
                                }
                            }
                            break;

                        case "LOG_REQUEST":
                            String logStr = logEventos.imprimir();
                            PaqueteDatos logResp = new PaqueteDatos("LOG_RESPONSE",
                                "ServidorPrincipal", logStr, null);
                            out.writeObject(logResp);
                            out.flush();
                            break;

                        case "VOICE_JOIN":
                            String canal = paqueteEntrada.getMensaje();
                            // Generar token de voz
                            String voiceToken = UUID.randomUUID().toString()
                                    .substring(0, Config.VOICE_TOKEN_LONGITUD);

                            // Registrar token en ServidorVoz via TCP (señalización)
                            boolean tokenRegistrado = registrarTokenEnServidorVoz(voiceToken);

                            if (tokenRegistrado) {
                                String infoVoz = Config.HOST_VOZ_PRIMARIO + ":"
                                        + Config.PUERTO_VOZ_PRIMARIO + ":" + voiceToken;
                                PaqueteDatos vozInfo = new PaqueteDatos("VOICE_INFO",
                                        "ServidorPrincipal", infoVoz, null);
                                out.writeObject(vozInfo);
                                out.flush();
                                System.out.println(usuario + " se unio al canal de voz: "
                                        + canal + " (token: " + voiceToken + ")");

                                relojLamport++;
                                logEventos.registrar(relojLamport,
                                    "ServidorPrincipal" + idNodo, "VOICE_TOKEN_GENERADO",
                                    "usuario=" + usuario + " token=" + voiceToken);
                            } else {
                                PaqueteDatos vozError = new PaqueteDatos("VOICE_INFO",
                                        "ServidorPrincipal",
                                        "ERROR:No se pudo registrar token de voz", null);
                                out.writeObject(vozError);
                                out.flush();
                                System.out.println("Error registrando token de voz para "
                                        + usuario);
                            }
                            break;

                        case "VOICE_LEAVE":
                            System.out.println(usuario + " salio del canal de voz.");
                            break;
                    }
                }

            } catch (EOFException | SocketException e) {
                System.out.println("Cliente desconectado: "
                        + (usuario != null ? usuario : "desconocido"));
            } catch (IOException | ClassNotFoundException e) {
                if (e.getMessage() != null && !e.getMessage().contains("Socket closed")
                        && !e.getMessage().contains("Connection reset")) {
                    System.out.println("Error con cliente: " + e.getMessage());
                }
            } finally {
                desconectarCliente();
            }
        }

        private void mantenerBackup() {
            try {
                PaqueteDatos p;
                while ((p = (PaqueteDatos) in.readObject()) != null) { }
            } catch (Exception e) {
                flujoSalidaReplicas.remove(out);
                System.out.println("Observador desconectado.");
            }
        }

        private void desconectarCliente() {
            try {
                flujoSalidaClientes.remove(out);
                flujoSalidaReplicas.remove(out);
                socket.close();
            } catch (IOException e) { }
            if (sessionId != null) {
                sesiones.remove(sessionId);
                replicarSesion(sessionId, usuario, "REMOVE");
                if (usuario != null) {
                    difundirUSER_LIST();
                }
            }
        }
    }

    // ─────────────────── SEÑALIZACIÓN DE VOZ (TOKENS) ────────────────

    private static boolean registrarTokenEnServidorVoz(String token) {
        int[] puertos = { Config.PUERTO_VOZ_TOKEN_PRIMARIO, Config.PUERTO_VOZ_TOKEN_BACKUP };

        for (int puertoToken : puertos) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(Config.HOST_VOZ_PRIMARIO, puertoToken), 2000);
                PrintWriter writer = new PrintWriter(s.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(s.getInputStream()));

                writer.println("REGISTER_TOKEN:" + token);
                String respuesta = reader.readLine();

                if ("TOKEN_REGISTERED".equals(respuesta)) {
                    System.out.println("Token registrado en ServidorVoz (puerto " + puertoToken + ")");
                    return true;
                }
            } catch (IOException e) {
                System.out.println("No se pudo registrar token en ServidorVoz puerto "
                        + puertoToken + ": " + e.getMessage());
            }
        }
        return false;
    }

    // ─────────────────── DIFUSION ───────────────────────────────────

    private static void difundirMensaje(PaqueteDatos mensaje) {
        synchronized (flujoSalidaClientes) {
            Iterator<ObjectOutputStream> iter = flujoSalidaClientes.iterator();
            while (iter.hasNext()) {
                ObjectOutputStream out = iter.next();
                try {
                    out.writeObject(mensaje);
                    out.flush();
                } catch (IOException e) {
                    iter.remove();
                }
            }
        }
        synchronized (flujoSalidaReplicas) {
            for (ObjectOutputStream out : flujoSalidaReplicas) {
                try {
                    out.writeObject(mensaje);
                    out.flush();
                } catch (IOException e) { }
            }
        }
    }

    private static void difundirUSER_LIST() {
        StringBuilder sb = new StringBuilder();
        synchronized (sesiones) {
            for (String user : sesiones.values()) {
                if (sb.length() > 0) sb.append(",");
                sb.append(user);
            }
        }
        PaqueteDatos p = new PaqueteDatos("USER_LIST", "ServidorPrincipal", sb.toString(), null);
        difundirMensaje(p);
    }

    // ─────────────────── REPLICACION DE SESIONES ────────────────────

    private static void replicarSesion(String sessionId, String usuario, String accion) {
        PaqueteDatos p = new PaqueteDatos("SESSION_SYNC", usuario,
                accion.equals("ADD") ? sessionId : "", null);
        synchronized (flujoSalidaReplicas) {
            for (ObjectOutputStream out : flujoSalidaReplicas) {
                try {
                    out.writeObject(p);
                    out.flush();
                } catch (IOException e) { }
            }
        }
    }

    private static void enviarEstadoCompletoSesiones(ObjectOutputStream out) {
        try {
            for (Map.Entry<String, String> entry : sesiones.entrySet()) {
                PaqueteDatos p = new PaqueteDatos("SESSION_SYNC",
                        entry.getValue(), entry.getKey(), null);
                out.writeObject(p);
                out.flush();
            }
            System.out.println("Estado de " + sesiones.size()
                    + " sesiones enviado al observador.");
        } catch (IOException e) {
            System.out.println("Error enviando estado: " + e.getMessage());
        }
    }

    // ─────────────────── RAFT ────────────────────────────────────────

    private static void proponerEntradaRaft(PaqueteDatos comando) {
        if (!soyCoordinador) return;

        RaftLogEntry entry = new RaftLogEntry(raftLog.size(), idNodo, comando);
        raftLog.add(entry);

        System.out.println("Raft: propongo entrada index=" + entry.index);

        relojLamport++;
        logEventos.registrar(relojLamport, "ServidorPrincipal" + idNodo,
                "PROPONER_RAFT", "index=" + entry.index);

        int[] peers = obtenerIdsPares();
        int totalNodos = peers.length + 1;
        int mayoria = totalNodos / 2 + 1;
        int acksFaltantes = mayoria - 1;

        raftAcksPendientes.clear();
        for (int id : peers) {
            raftAcksPendientes.add(String.valueOf(id));
        }

        RaftLogEntry[] entriesParaEnviar = new RaftLogEntry[]{entry};

        for (int id : peers) {
            final int peerId = id;
            pool.execute(() -> {
                PaqueteDatos resp = enviarAppendEntries(peerId, entriesParaEnviar, false);
                if (resp != null && resp.isRaftSuccess()) {
                    synchronized (raftLock) {
                        raftAcksPendientes.remove(String.valueOf(peerId));
                        raftLock.notifyAll();
                    }
                }
            });
        }

        synchronized (raftLock) {
            long deadline = System.currentTimeMillis() + Config.TIMEOUT_ELECCION_MS * 2;
            while (raftAcksPendientes.size() > peers.length - acksFaltantes
                    && System.currentTimeMillis() < deadline) {
                try { raftLock.wait(Math.max(1, deadline - System.currentTimeMillis())); }
                catch (InterruptedException e) { break; }
            }
        }

        committedRaftIndex = entry.index;
        System.out.println("Raft: entrada " + entry.index + " COMMITEADA");

        relojLamport++;
        logEventos.registrar(relojLamport, "ServidorPrincipal" + idNodo,
                "CONFIRMAR_RAFT", "entrada " + entry.index + " commiteada");
    }

    private static PaqueteDatos enviarAppendEntries(int idDestino, RaftLogEntry[] entries, boolean esHeartbeat) {
        PaqueteDatos pkt = new PaqueteDatos("RAFT_APPEND_ENTRIES",
                String.valueOf(idNodo), esHeartbeat ? "HEARTBEAT" : "", null);

        int prevLogIndex;
        int prevLogTerm;
        if (entries != null && entries.length > 0) {
            prevLogIndex = entries[0].index - 1;
        } else {
            prevLogIndex = raftLog.size() - 1;
        }
        if (prevLogIndex >= 0 && prevLogIndex < raftLog.size()) {
            prevLogTerm = raftLog.get(prevLogIndex).term;
        } else {
            prevLogTerm = 0;
        }

        pkt.setRaftTerm(currentRaftTerm);
        pkt.setRaftEntries(entries);
        pkt.setRaftPrevLogIndex(prevLogIndex);
        pkt.setRaftPrevLogTerm(prevLogTerm);
        pkt.setRaftLeaderCommit(committedRaftIndex);

        return enviarMensajeSP(idDestino, pkt, true, Config.TIMEOUT_ELECCION_MS);
    }

    private static void manejarAppendEntries(PaqueteDatos pkt, ObjectOutputStream out) {
        RaftLogEntry[] entries = pkt.getRaftEntries();
        int prevLogIndex = pkt.getRaftPrevLogIndex();
        int prevLogTerm = pkt.getRaftPrevLogTerm();
        int leaderCommit = pkt.getRaftLeaderCommit();

        boolean consistent = true;
        if (prevLogIndex >= 0) {
            if (prevLogIndex >= raftLog.size()) {
                consistent = false;
            } else {
                RaftLogEntry existing = raftLog.get(prevLogIndex);
                if (existing.term != prevLogTerm) {
                    consistent = false;
                }
            }
        }

        if (consistent && entries != null) {
            for (RaftLogEntry entry : entries) {
                if (entry.index < raftLog.size()) {
                    if (raftLog.get(entry.index).term != entry.term) {
                        raftLog.subList(entry.index, raftLog.size()).clear();
                        raftLog.add(entry);
                    }
                } else {
                    raftLog.add(entry);
                }
            }

            if (leaderCommit > committedRaftIndex) {
                committedRaftIndex = Math.min(leaderCommit, raftLog.size() - 1);
            }

            aplicarEntradasPendientes();

            System.out.println("Raft: recibidas entradas del lider " + pkt.getEmisor()
                    + " index=" + (entries.length > 0 ? entries[0].index : "heartbeat"));
        }

        PaqueteDatos resp = new PaqueteDatos("RAFT_APPEND_ENTRIES_RESPONSE",
                String.valueOf(idNodo), "", null);
        resp.setRaftSuccess(consistent);
        try {
            out.writeObject(resp);
            out.flush();
        } catch (IOException e) {
            System.out.println("Error respondiendo AppendEntries: " + e.getMessage());
        }
    }

    private static void aplicarEntradasPendientes() {
        while (lastAppliedRaftIndex < committedRaftIndex
                && lastAppliedRaftIndex + 1 < raftLog.size()) {
            lastAppliedRaftIndex++;
            RaftLogEntry entry = raftLog.get(lastAppliedRaftIndex);

            relojLamport++;
            logEventos.registrar(relojLamport, "ServidorPrincipal" + idNodo,
                    "APLICAR_RAFT", "aplicando entrada index=" + entry.index);

            PaqueteDatos p = entry.comando;
            if (p != null) {
                bufferHistorial.add(p);
                if (bufferHistorial.size() > Config.MAX_HISTORIAL_CHAT) {
                    bufferHistorial.remove(0);
                }
            }
        }
    }

    // ─────────────────── NUEVOS PARA 2.4 ─────────────────────────

    private static void enviarHistorialCliente(ObjectOutputStream out) {
        synchronized (bufferHistorial) {
            for (PaqueteDatos msg : bufferHistorial) {
                try {
                    out.writeObject(msg);
                    out.flush();
                } catch (IOException e) {
                    break;
                }
            }
        }
        System.out.println("Historial enviado al cliente (" + bufferHistorial.size() + " mensajes)");
    }

    private static void enviarRaftLogCompleto(ObjectOutputStream out) {
        try {
            PaqueteDatos p = new PaqueteDatos("RAFT_LOG_SYNC",
                    String.valueOf(idNodo), "", null);
            p.setRaftEntries(raftLog.toArray(new RaftLogEntry[0]));
            p.setRaftLeaderCommit(committedRaftIndex);
            out.writeObject(p);
            out.flush();
            System.out.println("Raft: log completo enviado al observador ("
                    + raftLog.size() + " entradas)");
        } catch (IOException e) {
            System.out.println("Error enviando raftLog: " + e.getMessage());
        }
    }

    private static void iniciarHeartbeatRaft() {
        detenerHeartbeatRaft();
        raftHeartbeatTimer = Executors.newSingleThreadScheduledExecutor();
        raftHeartbeatTimer.scheduleAtFixedRate(() -> {
            if (soyCoordinador) {
                for (int id : obtenerIdsPares()) {
                    final int peerId = id;
                    pool.execute(() -> enviarAppendEntries(peerId, null, true));
                }
            }
        }, Config.HEARTBEAT_INTERVALO_MS, Config.HEARTBEAT_INTERVALO_MS, TimeUnit.MILLISECONDS);
        System.out.println("Raft: heartbeat iniciado");
    }

    private static void detenerHeartbeatRaft() {
        if (raftHeartbeatTimer != null) {
            raftHeartbeatTimer.shutdownNow();
        }
        System.out.println("Raft: heartbeat detenido");
    }
}
