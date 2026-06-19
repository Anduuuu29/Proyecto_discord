import java.io.*;
import java.net.*;
import java.util.*;

public class ServidorVoz {

    private static Set<String> clientes = new HashSet<>();

    public static void main(String[] args) {
        boolean esBackup = args.length > 0 && args[0].equalsIgnoreCase("backup");
        int puerto = esBackup ? Config.PUERTO_BACKUP_VOZ : Config.PUERTO_PRIMARIO_VOZ;

        try (DatagramSocket socket = new DatagramSocket(puerto)) {
            System.out.println("Servidor de voz " + (esBackup ? "BACKUP" : "PRIMARIO")
                    + " corriendo en el puerto " + puerto);

            byte[] buffer = new byte[16384];

            while (true) {
                DatagramPacket paqueteRecibido = new DatagramPacket(buffer, buffer.length);
                socket.receive(paqueteRecibido);

                InetAddress direccionCliente = paqueteRecibido.getAddress();
                int puertoCliente = paqueteRecibido.getPort();
                String clienteID = direccionCliente.toString() + ":" + puertoCliente;

                clientes.add(clienteID);

                for (String cliente : clientes) {
                    String[] partes = cliente.split(":");
                    InetAddress direccion = InetAddress.getByName(partes[0].replace("/", ""));
                    int puertoDestino = Integer.parseInt(partes[1]);

                    if (!cliente.equals(clienteID)) {
                        DatagramPacket paqueteEnvio = new DatagramPacket(
                                paqueteRecibido.getData(),
                                paqueteRecibido.getLength(),
                                direccion,
                                puertoDestino
                        );
                        socket.send(paqueteEnvio);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error en el servidor de voz: " + e.getMessage());
        }
    }
}
