import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Cliente {

    private static final String[][] GATEWAYS = {
        { Config.HOST_GATEWAY_PRIMARIO, String.valueOf(Config.PUERTO_GATEWAY) },
        { Config.HOST_GATEWAY_BACKUP, String.valueOf(Config.PUERTO_GATEWAY_BACKUP) }
    };

    private static final String[][] PRINCIPALES = {
        { Config.HOST_PRINCIPAL_PRIMARIO, String.valueOf(Config.PUERTO_PRINCIPAL_PRIMARIO) },
        { Config.HOST_PRINCIPAL_BACKUP, String.valueOf(Config.PUERTO_PRINCIPAL_BACKUP) }
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
            sessionId = null;

            // ── Paso 1: Autenticar contra Gateway ──
            Socket socketGateway = null;
            ObjectInputStream gatewayIn = null;
            ObjectOutputStream gatewayOut = null;

            for (int i = 0; i < GATEWAYS.length; i++) {
                String host = GATEWAYS[i][0];
                int puerto = Integer.parseInt(GATEWAYS[i][1]);

                try {
                    socketGateway = new Socket(host, puerto);
                    System.out.println("Conectado a Gateway " + (i == 0 ? "PRIMARIO" : "BACKUP")
                            + " (" + host + ":" + puerto + ")");

                    gatewayIn = new ObjectInputStream(socketGateway.getInputStream());
                    gatewayOut = new ObjectOutputStream(socketGateway.getOutputStream());

                    PaqueteDatos login = new PaqueteDatos("AUTH", usuario, "", null);
                    gatewayOut.writeObject(login);
                    gatewayOut.flush();

                    PaqueteDatos respuesta = (PaqueteDatos) gatewayIn.readObject();
                    if (!"AUTH_OK".equals(respuesta.getTipo())) {
                        System.out.println("Error de autenticacion.");
                        socketGateway.close();
                        continue;
                    }

                    sessionId = respuesta.getSessionId();
                    String dirDestino = respuesta.getServidorDestino();
                    System.out.println("Autenticado como: " + respuesta.getMensaje()
                            + " (sesion: " + sessionId + ")");
                    System.out.println("Redirigiendo a: " + dirDestino);

                    // Cerramos conexion con Gateway
                    socketGateway.close();
                    break;

                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("No se pudo conectar a Gateway " + host + ":" + puerto);
                }
            }

            if (sessionId == null) {
                System.out.println("No se pudo autenticar. Reintentando en "
                        + (Config.TIEMPO_RECONEXION_MS / 1000) + " segundos...");
                try { Thread.sleep(Config.TIEMPO_RECONEXION_MS); } catch (InterruptedException e) { break; }
                continue;
            }

            // ── Paso 2: Conectar a ServidorPrincipal ──
            boolean conectadoAPrincipal = false;

            for (int j = 0; j < PRINCIPALES.length; j++) {
                String host = PRINCIPALES[j][0];
                int puerto = Integer.parseInt(PRINCIPALES[j][1]);

                try {
                    Socket socketPrincipal = new Socket(host, puerto);
                    System.out.println("Conectado a ServidorPrincipal "
                            + (j == 0 ? "PRIMARIO" : "BACKUP")
                            + " (" + host + ":" + puerto + ")");

                    ObjectInputStream in = new ObjectInputStream(socketPrincipal.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socketPrincipal.getOutputStream());

                    PaqueteDatos connect = new PaqueteDatos("CONNECT", usuario, "", null);
                    connect.setSessionId(sessionId);
                    out.writeObject(connect);
                    out.flush();

                    PaqueteDatos respuesta = (PaqueteDatos) in.readObject();
                    if (!"CONNECT_OK".equals(respuesta.getTipo())) {
                        System.out.println("Error de conexion con ServidorPrincipal.");
                        socketPrincipal.close();
                        continue;
                    }

                    System.out.println("Conectado al servidor principal. (sesion: " + sessionId + ")");
                    conectadoAPrincipal = true;

                    // ── Hilo de recepcion ──
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
                            System.out.println("\nDesconectado del ServidorPrincipal.");
                        }
                    });
                    hiloReceptor.start();

                    // ── Bucle de envio ──
                    System.out.println("Comandos:");
                    System.out.println("  /voz <canal>  - Unirse a canal de voz");
                    System.out.println("  /salir        - Desconectarse");
                    System.out.println("  Cualquier otro texto se envia como mensaje.");

                    while (true) {
                        String entrada = scanner.nextLine();

                        if (entrada.equalsIgnoreCase("/salir")) {
                            System.out.println("Cerrando cliente...");
                            detenerClienteVoz();
                            socketPrincipal.close();
                            return;
                        }

                        if (entrada.startsWith("/voz ")) {
                            String canal = entrada.substring(5).trim();
                            PaqueteDatos p = new PaqueteDatos("VOICE_JOIN", usuario, canal, null);
                            p.setSessionId(sessionId);
                            try {
                                out.writeObject(p);
                                out.flush();
                            } catch (IOException e) {
                                System.out.println("Error al enviar comando de voz.");
                                break;
                            }
                            continue;
                        }

                        PaqueteDatos texto = new PaqueteDatos("CHAT", usuario, entrada, null);
                        texto.setSessionId(sessionId);
                        try {
                            out.writeObject(texto);
                            out.flush();
                        } catch (IOException e) {
                            System.out.println("Conexion con ServidorPrincipal perdida.");
                            break;
                        }
                    }

                    // Si llegamos aqui, la conexion se perdio
                    socketPrincipal.close();
                    break;

                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("No se pudo conectar a ServidorPrincipal "
                            + host + ":" + puerto + " - " + e.getMessage());
                }
            }

            if (!conectadoAPrincipal) {
                System.out.println("Todos los ServidoresPrincipales fallaron. Reautenticando...");
            }

            try { Thread.sleep(500); } catch (InterruptedException e) { break; }
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
