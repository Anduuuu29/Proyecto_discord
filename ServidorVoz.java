import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class ServidorVoz {

    // Tokens válidos registrados por el ServidorPrincipal (vía TCP)
    private static Set<String> tokensValidos = ConcurrentHashMap.newKeySet();

    // Clientes que ya presentaron un token válido
    private static Set<String> clientesAutenticados = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        boolean esBackup = args.length > 0 && args[0].equalsIgnoreCase("backup");
        int puertoUDP = esBackup ? Config.PUERTO_VOZ_BACKUP : Config.PUERTO_VOZ_PRIMARIO;
        int puertoTCP = esBackup ? Config.PUERTO_VOZ_TOKEN_BACKUP : Config.PUERTO_VOZ_TOKEN_PRIMARIO;

        // ── Hilo TCP: recibe tokens válidos desde ServidorPrincipal ──
        Thread hiloTokens = new Thread(() -> {
            try (ServerSocket serverTCP = new ServerSocket(puertoTCP)) {
                System.out.println("ServidorVoz: Listener TCP de tokens en puerto " + puertoTCP);
                while (true) {
                    Socket socketPrincipal = serverTCP.accept();
                    // Cada conexión TCP trae un token para registrar
                    new Thread(() -> {
                        try {
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(socketPrincipal.getInputStream()));
                            PrintWriter writer = new PrintWriter(
                                    socketPrincipal.getOutputStream(), true);

                            String linea = reader.readLine();
                            if (linea != null && linea.startsWith("REGISTER_TOKEN:")) {
                                String token = linea.substring("REGISTER_TOKEN:".length());
                                tokensValidos.add(token);
                                writer.println("TOKEN_REGISTERED");
                                System.out.println("Token registrado: " + token
                                        + " (total: " + tokensValidos.size() + ")");
                            }

                            socketPrincipal.close();
                        } catch (IOException e) {
                            System.out.println("Error en conexión TCP de token: " + e.getMessage());
                        }
                    }).start();
                }
            } catch (IOException e) {
                System.out.println("Error en listener TCP de tokens: " + e.getMessage());
            }
        });
        hiloTokens.setDaemon(true);
        hiloTokens.start();

        // ── Bucle UDP principal (igual que antes, pero con validación) ──
        try (DatagramSocket socket = new DatagramSocket(puertoUDP)) {
            System.out.println("Servidor de voz " + (esBackup ? "BACKUP" : "PRIMARIO")
                    + " corriendo en el puerto " + puertoUDP);

            byte[] buffer = new byte[16384];

            while (true) {
                DatagramPacket paqueteRecibido = new DatagramPacket(buffer, buffer.length);
                socket.receive(paqueteRecibido);

                InetAddress direccionCliente = paqueteRecibido.getAddress();
                int puertoCliente = paqueteRecibido.getPort();
                String clienteID = direccionCliente.toString() + ":" + puertoCliente;

                byte[] datos = paqueteRecibido.getData();
                int longitud = paqueteRecibido.getLength();

                // ── AUTH: paquete 0x03 + token (autenticación del cliente) ──
                if (longitud > 1 && datos[0] == 0x03) {
                    String token = new String(datos, 1, longitud - 1).trim();
                    if (tokensValidos.contains(token)) {
                        clientesAutenticados.add(clienteID);
                        tokensValidos.remove(token); // Token de un solo uso
                        byte[] authOk = { 0x04 };
                        DatagramPacket respuesta = new DatagramPacket(
                                authOk, authOk.length, direccionCliente, puertoCliente);
                        socket.send(respuesta);
                        System.out.println("Cliente autenticado: " + clienteID
                                + " (token: " + token + ")");
                    } else {
                        System.out.println("Token inválido de " + clienteID
                                + ": " + token);
                    }
                    continue;
                }

                // ── Rechazar clientes no autenticados ──
                if (!clientesAutenticados.contains(clienteID)) {
                    // Ignorar silenciosamente
                    continue;
                }

                // ── PING -> responder PONG solo al emisor (no reenviar a otros) ──
                if (longitud == 1 && datos[0] == 0x01) {
                    byte[] pong = { 0x02 };
                    DatagramPacket paquetePong = new DatagramPacket(
                            pong, pong.length, direccionCliente, puertoCliente);
                    socket.send(paquetePong);
                    continue;
                }

                // ── Reenviar audio a todos los clientes autenticados (excepto al emisor) ──
                for (String cliente : clientesAutenticados) {
                    String[] partes = cliente.split(":");
                    InetAddress direccion = InetAddress.getByName(partes[0].replace("/", ""));
                    int puertoDestino = Integer.parseInt(partes[1]);

                    if (!cliente.equals(clienteID)) {
                        DatagramPacket paqueteEnvio = new DatagramPacket(
                                datos, longitud, direccion, puertoDestino);
                        socket.send(paqueteEnvio);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error en el servidor de voz: " + e.getMessage());
        }
    }
}
