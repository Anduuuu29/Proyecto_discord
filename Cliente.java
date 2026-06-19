import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Cliente {

    private static final String[][] SERVIDORES = {
        { Config.HOST_PRIMARIO_TEXTO, String.valueOf(Config.PUERTO_PRIMARIO_TEXTO) },
        { Config.HOST_BACKUP_TEXTO, String.valueOf(Config.PUERTO_BACKUP_TEXTO) }
    };

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Ingrese su nombre de usuario: ");
        String usuario = scanner.nextLine().trim();
        if (usuario.isEmpty()) {
            usuario = "Usuario";
        }

        while (true) {
            for (int i = 0; i < SERVIDORES.length; i++) {
                String host = SERVIDORES[i][0];
                int puerto = Integer.parseInt(SERVIDORES[i][1]);

                try {
                    Socket socket = new Socket(host, puerto);
                    System.out.println("Conectado al servidor " + (i == 0 ? "PRIMARIO" : "BACKUP")
                            + " (" + host + ":" + puerto + ")");

                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                    PaqueteDatos login = new PaqueteDatos("AUTH", usuario, "Inicio de sesion", null);
                    out.writeObject(login);
                    out.flush();

                    Thread hiloReceptor = new Thread(() -> {
                        try {
                            PaqueteDatos paqueteEntrada;
                            while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {
                                if (paqueteEntrada.getTipo().equals("CHAT")) {
                                    System.out.println("[" + paqueteEntrada.getEmisor() + "]: "
                                            + paqueteEntrada.getMensaje());
                                }
                            }
                        } catch (IOException | ClassNotFoundException e) {
                            System.out.println("\nDesconectado del servidor.");
                        }
                    });
                    hiloReceptor.start();

                    System.out.println("Conectado, ingrese sus mensajes ('salir' para desconectarse): ");

                    while (true) {
                        String mensaje = scanner.nextLine();
                        if (mensaje.equalsIgnoreCase("salir")) {
                            System.out.println("Cerrando cliente...");
                            socket.close();
                            return;
                        }
                        PaqueteDatos texto = new PaqueteDatos("CHAT", usuario, mensaje, null);
                        out.writeObject(texto);
                        out.flush();
                    }

                } catch (IOException e) {
                    System.out.println("No se pudo conectar a " + host + ":" + puerto
                            + " - " + e.getMessage());
                }
            }

            System.out.println("Todos los servidores fallaron. Reintentando en "
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
}
