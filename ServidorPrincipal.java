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

    // Replicacion (Backup)
    private static List<ObjectOutputStream> flujoSalidaReplicas =
            Collections.synchronizedList(new ArrayList<>());
    private static List<PaqueteDatos> bufferHistorial =
            Collections.synchronizedList(new ArrayList<>());
    private static boolean esBackup = false;
    private static volatile boolean primarioActivo = true;
    private static int delayMs = 0;

    // Conexion a ServidorChat
    private static Socket socketChat;
    private static ObjectOutputStream chatOut;
    private static volatile boolean chatConectado = false;

    public static void main(String[] args) {
        for (String arg : args) {
            if ("backup".equalsIgnoreCase(arg)) esBackup = true;
            else if (arg.matches("\\d+")) delayMs = Integer.parseInt(arg);
        }
        int puerto = esBackup ? Config.PUERTO_PRINCIPAL_BACKUP : Config.PUERTO_PRINCIPAL_PRIMARIO;

        if (esBackup) {
            iniciarReplicacion();
        }

        conectarServidorChat();

        try (ServerSocket serverSocket = new ServerSocket(puerto)) {
            String extra = delayMs > 0 ? " (delay: " + delayMs + "ms)" : "";
            System.out.println("ServidorPrincipal " + (esBackup ? "BACKUP" : "PRIMARIO")
                    + " corriendo en el puerto " + puerto + extra);

            while (true) {
                Socket socketCliente = serverSocket.accept();
                pool.execute(new ManejarCliente(socketCliente));
            }
        } catch (IOException e) {
            System.out.println("Error en ServidorPrincipal: " + e.getMessage());
        }
    }

    // ─────────────────────── CONEXION A SERVIDORCHAT ───────────────────────

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

    // ─────────────────────── REPLICACION (BACKUP) ───────────────────────

    private static void iniciarReplicacion() {
        Thread hilo = new Thread(() -> {
            while (true) {
                try (Socket s = new Socket(Config.HOST_PRINCIPAL_PRIMARIO, Config.PUERTO_PRINCIPAL_PRIMARIO);
                     ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

                    PaqueteDatos auth = new PaqueteDatos("AUTH", "PRINCIPAL_BACKUP", "", null);
                    out.writeObject(auth);
                    out.flush();
                    System.out.println("Backup conectado al ServidorPrincipal primario.");

                    PaqueteDatos p;
                    while ((p = (PaqueteDatos) in.readObject()) != null) {
                        switch (p.getTipo()) {
                            case "CHAT":
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
                        }
                    }
                } catch (EOFException | SocketException e) {
                    System.out.println("ServidorPrincipal primario no disponible. Backup asume el control.");
                    primarioActivo = false;
                    conectarServidorChat();
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Error en replicacion: " + e.getMessage());
                    primarioActivo = false;
                }

                if (!primarioActivo) break;

                try { Thread.sleep(Config.TIEMPO_RECONEXION_MS); } catch (InterruptedException ex) { break; }
            }
        });
        hilo.setDaemon(true);
        hilo.start();
    }

    // ─────────────────────── MANEJO DE CLIENTES ───────────────────────

    private static class ManejarCliente implements Runnable {
        private Socket socket;
        private ObjectOutputStream out;
        private ObjectInputStream in;
        private String sessionId;
        private String usuario;
        private boolean esBackupInterno = false;

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

                if (!"CONNECT".equals(paqueteEntrada.getTipo())
                        && !"AUTH".equals(paqueteEntrada.getTipo())) {
                    socket.close();
                    return;
                }

                if ("AUTH".equals(paqueteEntrada.getTipo())
                        && "PRINCIPAL_BACKUP".equals(paqueteEntrada.getEmisor())) {
                    flujoSalidaReplicas.add(out);
                    System.out.println("Backup de ServidorPrincipal conectado para replicacion.");
                    enviarEstadoCompletoSesiones(out);
                    mantenerBackup();
                    return;
                }

                if (!"CONNECT".equals(paqueteEntrada.getTipo())) {
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

                System.out.println("Cliente conectado a ServidorPrincipal: " + usuario
                        + " (session: " + sessionId + ")");

                PaqueteDatos confirmacion = new PaqueteDatos("CONNECT_OK", "ServidorPrincipal",
                        "Conectado al servidor principal", null);
                confirmacion.setSessionId(sessionId);
                out.writeObject(confirmacion);
                out.flush();

                replicarSesion(sessionId, usuario, "ADD");
                difundirUSER_LIST();

                // Enviar historial reciente si es backup promovido
                if (esBackup && !primarioActivo) {
                    enviarHistorial();
                }

                while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {
                    switch (paqueteEntrada.getTipo()) {
                        case "CHAT":
                            if (chatConectado) {
                                chatOut.writeObject(paqueteEntrada);
                                chatOut.flush();
                            } else {
                                System.out.println("ServidorChat no disponible, mensaje en buffer local.");
                                paqueteEntrada.setTimestamp(System.currentTimeMillis());
                                bufferHistorial.add(paqueteEntrada);
                                if (bufferHistorial.size() > Config.MAX_HISTORIAL_CHAT) {
                                    bufferHistorial.remove(0);
                                }
                                difundirMensaje(paqueteEntrada);
                            }
                            break;

                        case "VOICE_JOIN":
                            String canal = paqueteEntrada.getMensaje();
                            String infoVoz = Config.HOST_VOZ_PRIMARIO + ":" + Config.PUERTO_VOZ_PRIMARIO;
                            PaqueteDatos vozInfo = new PaqueteDatos("VOICE_INFO", "ServidorPrincipal",
                                    infoVoz, null);
                            out.writeObject(vozInfo);
                            out.flush();
                            System.out.println(usuario + " se unio al canal de voz: " + canal);
                            break;

                        case "VOICE_LEAVE":
                            System.out.println(usuario + " salio del canal de voz.");
                            break;
                    }
                }

            } catch (EOFException | SocketException e) {
                System.out.println("Cliente desconectado: " + (usuario != null ? usuario : "desconocido"));
            } catch (IOException | ClassNotFoundException e) {
                if (!e.getMessage().contains("Socket closed")) {
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
                System.out.println("Backup de ServidorPrincipal desconectado.");
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

        private void enviarHistorial() {
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
        }
    }

    // ─────────────────────── DIFUSION ───────────────────────

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

    // ─────────────────────── REPLICACION DE SESIONES ───────────────────────

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
                PaqueteDatos p = new PaqueteDatos("SESSION_SYNC", entry.getValue(), entry.getKey(), null);
                out.writeObject(p);
                out.flush();
            }
            System.out.println("Estado de " + sesiones.size() + " sesiones enviado al backup.");
        } catch (IOException e) {
            System.out.println("Error enviando estado al backup: " + e.getMessage());
        }
    }
}
