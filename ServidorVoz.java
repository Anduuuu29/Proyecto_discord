import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class ServidorVoz {

    // Mapea: token -> canal
    private static Map<String, String> tokenACanal = new ConcurrentHashMap<>();

    // Mapea: clienteID (IP:puerto) -> canal
    private static Map<String, String> clienteACanal = new ConcurrentHashMap<>();

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
                    new Thread(() -> {
                        try {
                            BufferedReader reader = new BufferedReader(
                                    new InputStreamReader(socketPrincipal.getInputStream()));
                            PrintWriter writer = new PrintWriter(
                                    socketPrincipal.getOutputStream(), true);

                            String linea = reader.readLine();
                            if (linea != null && linea.startsWith("REGISTER_TOKEN:")) {
                                String resto = linea.substring("REGISTER_TOKEN:".length());
                                String[] partes = resto.split(":");
                                if (partes.length >= 2) {
                                    String token = partes[0];
                                    String canal = partes[1];
                                    tokenACanal.put(token, canal);
                                    writer.println("TOKEN_REGISTERED");
                                    System.out.println("Token registrado: " + token + " -> canal: " + canal);
                                }
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

        // ── Bucle UDP principal ──
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

                // ── AUTH: paquete 0x03 + token ──
                if (longitud > 1 && datos[0] == 0x03) {
                    String token = new String(datos, 1, longitud - 1).trim();
                    if (tokenACanal.containsKey(token)) {
                        String canal = tokenACanal.remove(token);
                        clienteACanal.put(clienteID, canal);
                        byte[] authOk = { 0x04 };
                        DatagramPacket respuesta = new DatagramPacket(
                                authOk, authOk.length, direccionCliente, puertoCliente);
                        socket.send(respuesta);
                        System.out.println("Cliente autenticado: " + clienteID + " en canal: " + canal);
                    } else {
                        System.out.println("Token inválido de " + clienteID + ": " + token);
                    }
                    continue;
                }

                // ── Rechazar clientes no autenticados ──
                if (!clienteACanal.containsKey(clienteID)) {
                    continue;
                }

                // ── PING ──
                if (longitud == 1 && datos[0] == 0x01) {
                    byte[] pong = { 0x02 };
                    DatagramPacket paquetePong = new DatagramPacket(
                            pong, pong.length, direccionCliente, puertoCliente);
                    socket.send(paquetePong);
                    continue;
                }

                // ── Reenviar audio solo a los clientes del mismo canal ──
                String canalEmisor = clienteACanal.get(clienteID);
                if (canalEmisor != null) {
                    for (Map.Entry<String, String> entry : clienteACanal.entrySet()) {
                        String clienteDestino = entry.getKey();
                        String canalDestino = entry.getValue();

                        if (canalDestino.equals(canalEmisor) && !clienteDestino.equals(clienteID)) {
                            String[] partes = clienteDestino.split(":");
                            InetAddress direccion = InetAddress.getByName(partes[0].replace("/", ""));
                            int puertoDestino = Integer.parseInt(partes[1]);
                            DatagramPacket paqueteEnvio = new DatagramPacket(
                                    datos, longitud, direccion, puertoDestino);
                            socket.send(paqueteEnvio);
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("Error en el servidor de voz: " + e.getMessage());
        }
    }
}
