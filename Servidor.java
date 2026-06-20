import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Servidor {
    private static final ExecutorService pool = Executors.newFixedThreadPool(100);
    private static List<ObjectOutputStream> flujoSalidaClientes = Collections.synchronizedList(new ArrayList<>());
    private static boolean esBackup = false;

    private static List<PaqueteDatos> bufferHistorial = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_HISTORIAL = Config.MAX_HISTORIAL_CHAT;

    private static int relojLamport = 0;
    private static LogEvento logEventos = new LogEvento();

    public static void main(String[] args) {
        esBackup = args.length > 0 && args[0].equalsIgnoreCase("backup");
        int puerto = esBackup ? Config.PUERTO_TEXTO_BACKUP : Config.PUERTO_TEXTO_PRIMARIO;

        if (esBackup) {
            iniciarReplicacion();
        }

        try (ServerSocket serverSocket = new ServerSocket(puerto)) {
            System.out.println("ServidorChat " + (esBackup ? "BACKUP" : "PRIMARIO")
                    + " corriendo en el puerto " + puerto);

            while (true) {
                Socket socketCliente = serverSocket.accept();
                System.out.println("Conexion aceptada desde " + socketCliente.getInetAddress());
                pool.execute(new ManejarConexion(socketCliente));
            }
        } catch (IOException e) {
            System.out.println("Error en ServidorChat: " + e.getMessage());
        }
    }

    private static void iniciarReplicacion() {
        Thread hiloReplicacion = new Thread(() -> {
            while (true) {
                try (Socket socket = new Socket(Config.HOST_TEXTO_PRIMARIO, Config.PUERTO_TEXTO_PRIMARIO);
                     ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                    PaqueteDatos auth = new PaqueteDatos("AUTH", "BACKUP", "Replicacion en curso", null);
                    out.writeObject(auth);
                    out.flush();
                    System.out.println("Backup conectado al ServidorChat primario.");

                    PaqueteDatos paquete;
                    while ((paquete = (PaqueteDatos) in.readObject()) != null) {
                        if (paquete.getTipo().equals("CHAT")) {
                            if (paquete.getRelojLamport() > relojLamport)
                                relojLamport = paquete.getRelojLamport();
                            relojLamport++;
                            logEventos.registrar(relojLamport,
                                "ServidorChatBackup", "REPLICAR_CHAT",
                                paquete.getEmisor() + ": " + paquete.getMensaje());
                            bufferHistorial.add(paquete);
                            if (bufferHistorial.size() > MAX_HISTORIAL) {
                                bufferHistorial.remove(0);
                            }
                        }
                    }
                } catch (EOFException | SocketException e) {
                    System.out.println("ServidorChat primario no disponible. Backup listo para asumir.");
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Error en replicacion: " + e.getMessage());
                }

                try {
                    Thread.sleep(Config.TIEMPO_RECONEXION_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        hiloReplicacion.setDaemon(true);
        hiloReplicacion.start();
    }

    private static class ManejarConexion implements Runnable {
        private Socket socket;
        private ObjectOutputStream out;
        private ObjectInputStream in;

        public ManejarConexion(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
                flujoSalidaClientes.add(out);

                PaqueteDatos paqueteEntrada;

                while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {

                    switch (paqueteEntrada.getTipo()) {
                        case "AUTH":
                            System.out.println("Autenticando conexion: " + paqueteEntrada.getEmisor());
                            break;
                        case "CHAT":
                            if (paqueteEntrada.getRelojLamport() > relojLamport)
                                relojLamport = paqueteEntrada.getRelojLamport();
                            relojLamport++;
                            paqueteEntrada.setRelojLamport(relojLamport);
                            logEventos.registrar(relojLamport, "ServidorChat",
                                "PROCESAR_CHAT", paqueteEntrada.getEmisor()
                                + ": " + paqueteEntrada.getMensaje());
                            paqueteEntrada.setTimestamp(System.currentTimeMillis());
                            System.out.println("[" + paqueteEntrada.getTimestamp() + "] Chat de "
                                    + paqueteEntrada.getEmisor() + ": " + paqueteEntrada.getMensaje());
                            bufferHistorial.add(paqueteEntrada);
                            if (bufferHistorial.size() > MAX_HISTORIAL) {
                                bufferHistorial.remove(0);
                            }
                            difundirMensaje(paqueteEntrada);
                            break;
                    }
                }
            } catch (EOFException | SocketException e) {
                System.out.println("Conexion cerrada.");
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Error de comunicacion: " + e.getMessage());
            } finally {
                try {
                    flujoSalidaClientes.remove(out);
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void difundirMensaje(PaqueteDatos mensaje) {
        synchronized (flujoSalidaClientes) {
            Iterator<ObjectOutputStream> iter = flujoSalidaClientes.iterator();
            while (iter.hasNext()) {
                ObjectOutputStream out = iter.next();
                try {
                    out.writeObject(mensaje);
                    out.flush();
                } catch (IOException e) {
                    System.out.println("Error enviando mensaje, eliminando conexion.");
                    iter.remove();
                }
            }
        }
    }
}
