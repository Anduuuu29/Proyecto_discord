import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Gateway {

    private static final ExecutorService pool = Executors.newFixedThreadPool(100);

    // Clientes reales conectados
    private static List<ObjectOutputStream> flujoSalidaClientes =
            Collections.synchronizedList(new ArrayList<>());

    // Conexiones de replicacion (Gateway Backup)
    private static List<ObjectOutputStream> flujoSalidaReplicas =
            Collections.synchronizedList(new ArrayList<>());

    // Sesiones activas: sessionId -> username
    private static Map<String, String> sesiones = new ConcurrentHashMap<>();

    // Buffer de historial para el backup
    private static List<PaqueteDatos> bufferHistorial =
            Collections.synchronizedList(new ArrayList<>());

    private static boolean esBackup = false;
    private static volatile boolean primarioActivo = true;

    public static void main(String[] args) {
        esBackup = args.length > 0 && args[0].equalsIgnoreCase("backup");
        int puerto = esBackup ? Config.PUERTO_GATEWAY_BACKUP : Config.PUERTO_GATEWAY;

        if (esBackup) {
            iniciarReplicacion();
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

    // ─────────────────────── REPLICACION (BACKUP) ───────────────────────

    private static void iniciarReplicacion() {
        Thread hilo = new Thread(() -> {
            while (true) {
                try (Socket s = new Socket(Config.HOST_GATEWAY_PRIMARIO, Config.PUERTO_GATEWAY);
                     ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

                    out.writeObject(new PaqueteDatos("AUTH", "GATEWAY_BACKUP", "", null));
                    out.flush();
                    System.out.println("Backup conectado al Gateway primario.");

                    PaqueteDatos p;
                    while ((p = (PaqueteDatos) in.readObject()) != null) {
                        switch (p.getTipo()) {
                            case "CHAT":
                                bufferHistorial.add(p);
                                if (bufferHistorial.size() > Config.MAX_HISTORIAL_CHAT) {
                                    bufferHistorial.remove(0);
                                }
                                difundirMensaje(p);
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
                    System.out.println("Gateway primario no disponible. Backup asume el control.");
                    primarioActivo = false;
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Error en replicacion: " + e.getMessage());
                    primarioActivo = false;
                }

                if (!primarioActivo) break;

                try {
                    Thread.sleep(Config.TIEMPO_RECONEXION_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
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

        public ManejarCliente(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());

                // Primer paquete debe ser AUTH
                PaqueteDatos paqueteEntrada = (PaqueteDatos) in.readObject();

                if (!"AUTH".equals(paqueteEntrada.getTipo())) {
                    socket.close();
                    return;
                }

                // ── Conexion del Gateway Backup ──
                if ("GATEWAY_BACKUP".equals(paqueteEntrada.getEmisor())) {
                    flujoSalidaReplicas.add(out);
                    System.out.println("Gateway BACKUP conectado para replicacion.");
                    enviarEstadoCompletoSesiones(out);
                    mantenerConexionBackup();
                    return;
                }

                // ── Cliente real ──
                usuario = paqueteEntrada.getEmisor();
                sessionId = UUID.randomUUID().toString().substring(0, 8);
                sesiones.put(sessionId, usuario);
                flujoSalidaClientes.add(out);

                PaqueteDatos authOk = new PaqueteDatos("AUTH_OK", "Gateway", usuario, null);
                authOk.setSessionId(sessionId);
                out.writeObject(authOk);
                out.flush();

                replicarSesion(sessionId, usuario, "ADD");
                System.out.println("Usuario conectado: " + usuario + " (session: " + sessionId + ")");
                difundirUSER_LIST();

                // Enviar historial si es backup promovido
                if (esBackup && !primarioActivo) {
                    enviarHistorial();
                }

                // ── Bucle principal de mensajes ──
                while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {
                    switch (paqueteEntrada.getTipo()) {
                        case "CHAT":
                            paqueteEntrada.setTimestamp(System.currentTimeMillis());
                            paqueteEntrada.setSessionId(sessionId);
                            System.out.println("[" + paqueteEntrada.getTimestamp() + "] " + usuario + ": "
                                    + paqueteEntrada.getMensaje());
                            difundirMensaje(paqueteEntrada);
                            break;

                        case "VOICE_JOIN":
                            String canal = paqueteEntrada.getMensaje();
                            String infoVoz = seleccionarServidorVoz();
                            PaqueteDatos vozInfo = new PaqueteDatos("VOICE_INFO", "Gateway", infoVoz, null);
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

        private void mantenerConexionBackup() {
            try {
                PaqueteDatos p;
                while ((p = (PaqueteDatos) in.readObject()) != null) {
                    // Solo mantener abierto, no se esperan mensajes del backup
                }
            } catch (Exception e) {
                flujoSalidaReplicas.remove(out);
                System.out.println("Gateway backup desconectado.");
            }
        }

        private void desconectarCliente() {
            try {
                flujoSalidaClientes.remove(out);
                flujoSalidaReplicas.remove(out);
                socket.close();
            } catch (IOException e) {
                // ignorar
            }
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

    // ─────────────────────── SERVIDOR DE VOZ ───────────────────────

    private static String seleccionarServidorVoz() {
        return Config.HOST_VOZ_PRIMARIO + ":" + Config.PUERTO_VOZ_PRIMARIO;
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
                } catch (IOException e) {
                    // replica caida, se limpia sola
                }
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
        PaqueteDatos p = new PaqueteDatos("USER_LIST", "Gateway", sb.toString(), null);
        difundirMensaje(p);
    }

    // ─────────────────────── REPLICACION DE SESIONES ───────────────────────

    private static void replicarSesion(String sessionId, String usuario, String accion) {
        PaqueteDatos p = new PaqueteDatos("SESSION_SYNC", usuario, accion.equals("ADD") ? sessionId : "", null);
        synchronized (flujoSalidaReplicas) {
            for (ObjectOutputStream out : flujoSalidaReplicas) {
                try {
                    out.writeObject(p);
                    out.flush();
                } catch (IOException e) {
                    // replica caida
                }
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
