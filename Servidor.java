import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Servidor {
    private static final int PUERTO = 5000;
    private static final ExecutorService pool = Executors.newFixedThreadPool(100);
    private static List<ObjectOutputStream> flujoSalidaClientes = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PUERTO)) {
            System.out.println("Servidor corriendo en el puerto " + PUERTO);

            while (true) {
                Socket socketCliente = serverSocket.accept();
                System.out.println("Cliente conectado desde " + socketCliente.getInetAddress());

                // Thread thread = new Thread(new ManejarCliente(socketCliente));
                // thread.start();
                pool.execute(new ManejarCliente(socketCliente));
            }
        } catch (IOException e) {
            System.out.println("Error de comunicacion con el cliente: " + e.getMessage());
        }
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

                PaqueteDatos paqueteEntrada;

                while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {

                    switch (paqueteEntrada.getTipo()) {
                        case "AUTH":
                            System.out.println("Autenticando usuario: " + paqueteEntrada.getEmisor());
                            break;
                        case "CHAT":
                            System.out.println("Mensaje de texto de " + paqueteEntrada.getEmisor() + ": "
                                    + paqueteEntrada.getMensaje());
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

        private void difundirMensaje(PaqueteDatos mensaje) {
            synchronized (flujoSalidaClientes) {
                for (ObjectOutputStream out : flujoSalidaClientes) {
                    try {
                        out.writeObject(mensaje);
                        out.flush();
                    } catch (IOException e) {
                        System.out.println("Error enviando mensaje: " + e.getMessage());
                    }
                }
            }

        }

    }

}
