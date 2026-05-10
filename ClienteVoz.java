import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Scanner;
import javax.sound.sampled.*;

public class ClienteVoz {


    private static final int PUERTO_SERVIDOR = 6000;


    private static final String HOST = "localhost";

    public static void main(String[] args) {


        try {

            
            DatagramSocket socket = new DatagramSocket();
            AudioFormat formato = new AudioFormat(
            16000.0f, // frecuencia
            16,       // bits
            1,        // mono
            true,     // signed
            false     // little endian
            );

            TargetDataLine microfono =AudioSystem.getTargetDataLine(formato);
            microfono.open(formato);
            microfono.start();


            SourceDataLine parlantes =
            AudioSystem.getSourceDataLine(formato);
            parlantes.open(formato);
            parlantes.start();

            InetAddress direccionServidor =
                    InetAddress.getByName(HOST);

            System.out.println("Cliente de voz iniciado.");

        
            Thread hiloRecepcion = new Thread(() -> {

                try {

                    byte[] bufferRecepcion = new byte[16384];

                    while (true) {

                        DatagramPacket paqueteRecibido =
                                new DatagramPacket(bufferRecepcion,bufferRecepcion.length);

                        socket.receive(paqueteRecibido);
                        parlantes.write(
                        paqueteRecibido.getData(),
                        0,
                        paqueteRecibido.getLength()
                    );

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            });

            hiloRecepcion.start();

            Scanner scanner = new Scanner(System.in);


            while (true) {

                byte[] buffer = new byte[4096];
                

                microfono.read(buffer, 0, buffer.length);

                DatagramPacket paqueteEnvio =
                        new DatagramPacket(
                                buffer,
                                buffer.length,
                                direccionServidor,
                                PUERTO_SERVIDOR
                        );

                socket.send(paqueteEnvio);

                Thread.sleep(20);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}