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

    public static void main(String[] args) {
        esBackup = args.length > 0 && args[0].equalsIgnoreCase("backup");
        int puerto = esBackup ? Config.PUERTO_TEXTO_BACKUP : Config.PUERTO_TEXTO_PRIMARIO;

        if (esBackup) {
            iniciarReplicacion();
        }

        try (ServerSocket serverSocket = new ServerSocket(puerto)) {
            System.out.println("Servidor de texto " + (esBackup ? "BACKUP" : "PRIMARIO")
                    + " corriendo en el puerto " + puerto);

            while (true) {
                Socket socketCliente = serverSocket.accept();
                System.out.println("Cliente conectado desde " + socketCliente.getInetAddress());
                pool.execute(new ManejarCliente(socketCliente));
            }
        } catch (IOException e) {
            System.out.println("Error en el servidor: " + e.getMessage());
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
                    System.out.println("Backup conectado al primario para replicacion.");

                    PaqueteDatos paquete;
                    while ((paquete = (PaqueteDatos) in.readObject()) != null) {
                        if (paquete.getTipo().equals("CHAT")) {
                            bufferHistorial.add(paquete);
                            if (bufferHistorial.size() > MAX_HISTORIAL) {
                                bufferHistorial.remove(0);
                            }
                            difundirMensaje(paquete);
                        }
                    }
                } catch (EOFException | SocketException e) {
                    System.out.println("Primario no disponible. Backup listo para asumir.");
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

    private static class ManejarCliente implements Runnable {
        private Socket socket;
        private ObjectOutputStream out;
        private ObjectInputStream in;

        public ManejarCliente(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                out = new ObjectOutputStream(socket.getOutputStream());
                out.flush();
                in = new ObjectInputStream(socket.getInputStream());
                flujoSalidaClientes.add(out);

                if (esBackup) {
                    enviarHistorial();
                }

                PaqueteDatos paqueteEntrada;

                while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {

                    switch (paqueteEntrada.getTipo()) {
                        case "AUTH":
                            System.out.println("Autenticando usuario: " + paqueteEntrada.getEmisor());
                            break;
                        case "CHAT":
                            paqueteEntrada.setTimestamp(System.currentTimeMillis());
                            System.out.println("[" + paqueteEntrada.getTimestamp() + "] Mensaje de "
                                    + paqueteEntrada.getEmisor() + ": " + paqueteEntrada.getMensaje());
                            difundirMensaje(paqueteEntrada);
                            break;
                    }
                }
            } catch (EOFException | SocketException e) {
                System.out.println("Cliente desconectado: " + e.getMessage());
            } catch (IOException | ClassNotFoundException e) {
                System.out.println("Error de comunicacion con el cliente: " + e.getMessage());
            } finally {
                try {
                    flujoSalidaClientes.remove(out);
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
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
            if (!bufferHistorial.isEmpty()) {
                System.out.println("Historial enviado a nuevo cliente: " + bufferHistorial.size() + " mensajes.");
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
                    System.out.println("Error enviando mensaje, eliminando cliente.");
                    iter.remove();
                }
            }
        }
    }
}
