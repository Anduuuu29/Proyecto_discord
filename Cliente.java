import java.io.*;
import java.net.*;
import java.util.Scanner;

public class Cliente {

    private static final int PUERTO = 5000;

    public static void main(String[] args) {

        try (
                Socket socket = new Socket("localhost", PUERTO)) {
            System.out.println("Conectado al Gateway/Servidor");

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

            PaqueteDatos login = new PaqueteDatos("AUTH", "UserTest", "Inicio de sesion", null);
            out.writeObject(login);
            out.flush();

            Thread hiloReceptor = new Thread(() -> {
                try {
                    PaqueteDatos paqueteEntrada;
                    while ((paqueteEntrada = (PaqueteDatos) in.readObject()) != null) {
                        if (paqueteEntrada.getTipo().equals("CHAT")) {
                            System.out.println("[" + paqueteEntrada.getEmisor() + "]: " + paqueteEntrada.getMensaje());
                        }
                    }
                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Desconectado del servidor.");
                }
            });

            hiloReceptor.start();

            Scanner scanner = new Scanner(System.in);
            System.out.println("Conectado, ingrese sus mensajes(Pon 'salir' para desconectarse): ");

            while (true) {
                String mensaje = scanner.nextLine();
                if (mensaje.equalsIgnoreCase("salir")) {
                    break;
                }
                PaqueteDatos texto = new PaqueteDatos("CHAT", "UserTest", mensaje, null);
                out.writeObject(texto);
                out.flush();

            }
        } catch (IOException e) {
            System.out.println("Error al conectar con el servidor: " + e.getMessage());
        }

    }

}
