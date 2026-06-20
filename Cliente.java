import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Cliente {

    private static final String[][] GATEWAYS = {
        { Config.HOST_GATEWAY_PRIMARIO, String.valueOf(Config.PUERTO_GATEWAY) },
        { Config.HOST_GATEWAY_BACKUP, String.valueOf(Config.PUERTO_GATEWAY_BACKUP) }
    };

    private static String sessionId;
    private static Process procesoVoz;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Ingrese su nombre de usuario: ");
        String usuario = scanner.nextLine().trim();
        if (usuario.isEmpty()) {
            usuario = "Usuario";
        }

        while (true) {
            for (int i = 0; i < GATEWAYS.length; i++) {
                String host = GATEWAYS[i][0];
                int puerto = Integer.parseInt(GATEWAYS[i][1]);

                try {
                    Socket socket = new Socket(host, puerto);
                    System.out.println("Conectado a Gateway " + (i == 0 ? "PRIMARIO" : "BACKUP")
                            + " (" + host + ":" + puerto + ")");

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    PaqueteDatos login = new PaqueteDatos("AUTH", usuario, "", null);
                    out.writeObject(login);
                    out.flush();

                    PaqueteDatos respuesta = (PaqueteDatos) in.readObject();
                    if (!"AUTH_OK".equals(respuesta.getTipo())) {
                        System.out.println("Error de autenticacion.");
                        socket.close();
                        continue;
                    }
                    sessionId = respuesta.getSessionId();
                    System.out.println("Autenticado como: " + respuesta.getMensaje()
                            + " (sesion: " + sessionId + ")");

                    Thread hiloReceptor = new Thread(() -> {
                        try {
                            PaqueteDatos paqueteEntrada;
                            while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {
                                switch (paqueteEntrada.getTipo()) {
                                    case "CHAT":
                                        System.out.println("[" + paqueteEntrada.getEmisor() + "]: "
                                                + paqueteEntrada.getMensaje());
                                        break;
                                    case "USER_LIST":
                                        System.out.println("\n--- Usuarios conectados: "
                                                + paqueteEntrada.getMensaje() + " ---");
                                        break;
                                    case "VOICE_INFO":
                                        String[] p1 = paqueteEntrada.getMensaje().split(":");
                                        System.out.println("\n--- Conectando a voz: "
                                                + p1[0] + ":" + p1[1] + " ---");
                                        iniciarClienteVoz(p1[0], p1[1]);
                                        break;
                                    case "VOICE_SWITCH":
                                        String[] p2 = paqueteEntrada.getMensaje().split(":");
                                        System.out.println("\n--- Servidor de voz cambiado a: "
                                                + p2[0] + ":" + p2[1] + " ---");
                                        iniciarClienteVoz(p2[0], p2[1]);
                                        break;
                                }
                            }
                        } catch (IOException | ClassNotFoundException e) {
                            System.out.println("\nDesconectado del Gateway.");
                        }
                    });
                    hiloReceptor.start();

                    System.out.println("Comandos:");
                    System.out.println("  /voz <canal>  - Unirse a canal de voz");
                    System.out.println("  /salir        - Desconectarse");
                    System.out.println("  Cualquier otro texto se envia como mensaje.");

                    while (true) {
                        String entrada = scanner.nextLine();

                        if (entrada.equalsIgnoreCase("/salir")) {
                            System.out.println("Cerrando cliente...");
                            detenerClienteVoz();
                            socket.close();
                            return;
                        }

                        if (entrada.startsWith("/voz ")) {
                            String canal = entrada.substring(5).trim();
                            PaqueteDatos p = new PaqueteDatos("VOICE_JOIN", usuario, canal, null);
                            p.setSessionId(sessionId);
                            out.writeObject(p);
                            out.flush();
                            continue;
                        }

                        PaqueteDatos texto = new PaqueteDatos("CHAT", usuario, entrada, null);
                        texto.setSessionId(sessionId);
                        out.writeObject(texto);
                        out.flush();
                    }

                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("No se pudo conectar a " + host + ":" + puerto
                            + " - " + e.getMessage());
                }
            }

            System.out.println("Todos los Gateways fallaron. Reintentando en "
                    + (Config.TIEMPO_RECONEXION_MS / 1000) + " segundos...");
            try {
                Thread.sleep(Config.TIEMPO_RECONEXION_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        scanner.close();
    }

    private static void iniciarClienteVoz(String host, String puerto) {
        detenerClienteVoz();
        try {
            String javaBin = System.getProperty("java.home") + File.separator
                    + "bin" + File.separator + "java";
            ProcessBuilder pb = new ProcessBuilder(
                    javaBin, "-cp", ".", "ClienteVoz", host, puerto);
            pb.inheritIO();
            procesoVoz = pb.start();
            System.out.println("Cliente de voz iniciado en " + host + ":" + puerto);
        } catch (IOException e) {
            System.out.println("Error al iniciar ClienteVoz: " + e.getMessage());
        }
    }

    private static void detenerClienteVoz() {
        if (procesoVoz != null && procesoVoz.isAlive()) {
            procesoVoz.destroyForcibly();
            System.out.println("Cliente de voz anterior detenido.");
        }
    }
}
