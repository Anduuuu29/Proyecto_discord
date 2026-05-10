import java.io.*;
import java.net.*;
import java.util.*;

public class ServidorVoz {

    private static final int PUERTO = 6000;

    // Lista de clientes conectados
    private static Set<String> clientes = new HashSet<>();

    public static void main(String[] args) {

        try (DatagramSocket socket = new DatagramSocket(PUERTO)) {

            System.out.println("Servidor de voz corriendo en el puerto " + PUERTO);

            byte[] buffer = new byte[16384];

            while (true) {

                DatagramPacket paqueteRecibido =
                        new DatagramPacket(buffer, buffer.length);


                socket.receive(paqueteRecibido);

                InetAddress direccionCliente =
                        paqueteRecibido.getAddress();

                int puertoCliente =
                        paqueteRecibido.getPort();


                String clienteID =
                        direccionCliente.toString() + ":" + puertoCliente;

                clientes.add(clienteID);

                System.out.println("Paquete recibido de: " + clienteID);


                for (String cliente : clientes) {

                    String[] partes = cliente.split(":");

                    InetAddress direccion =
                            InetAddress.getByName(
                                    partes[0].replace("/", "")
                            );

                    int puerto =
                            Integer.parseInt(partes[1]);

                    if (!cliente.equals(clienteID)) {

                        DatagramPacket paqueteEnvio =
                                new DatagramPacket(
                                        paqueteRecibido.getData(),
                                        paqueteRecibido.getLength(),
                                        direccion,
                                        puerto
                                );

                        socket.send(paqueteEnvio);
                    }
                }
            }

        } catch (IOException e) {

            System.out.println(
                    "Error en el servidor de voz: "
                            + e.getMessage()
            );
        }
    }
}