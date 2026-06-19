import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import javax.sound.sampled.*;

public class ClienteVoz {

    private static final String[][] SERVIDORES = {
        { Config.HOST_PRIMARIO_VOZ, String.valueOf(Config.PUERTO_PRIMARIO_VOZ) },
        { Config.HOST_BACKUP_VOZ, String.valueOf(Config.PUERTO_BACKUP_VOZ) }
    };

    private static volatile int servidorActual = 0;
    private static volatile long ultimoPaqueteRecibido = System.currentTimeMillis();

    public static void main(String[] args) {
        try {
            DatagramSocket socket = new DatagramSocket();
            AudioFormat formato = new AudioFormat(
                    16000.0f,
                    16,
                    1,
                    true,
                    false
            );

            TargetDataLine microfono = AudioSystem.getTargetDataLine(formato);
            microfono.open(formato);
            microfono.start();

            SourceDataLine parlantes = AudioSystem.getSourceDataLine(formato);
            parlantes.open(formato);
            parlantes.start();

            InetAddress direccionServidor = InetAddress.getByName(SERVIDORES[servidorActual][0]);
            int puertoServidor = Integer.parseInt(SERVIDORES[servidorActual][1]);

            System.out.println("Cliente de voz iniciado. Conectando a "
                    + SERVIDORES[servidorActual][0] + ":" + puertoServidor);

            Thread hiloRecepcion = new Thread(() -> {
                try {
                    byte[] bufferRecepcion = new byte[16384];
                    while (true) {
                        DatagramPacket paqueteRecibido = new DatagramPacket(
                                bufferRecepcion, bufferRecepcion.length);
                        socket.receive(paqueteRecibido);
                        parlantes.write(paqueteRecibido.getData(), 0, paqueteRecibido.getLength());
                        ultimoPaqueteRecibido = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    System.out.println("Error en recepcion de audio.");
                }
            });
            hiloRecepcion.start();

            while (true) {
                long ahora = System.currentTimeMillis();
                if (ahora - ultimoPaqueteRecibido > Config.TIMEOUT_VOZ_MS) {
                    int nuevoServidor = (servidorActual + 1) % SERVIDORES.length;
                    if (nuevoServidor != servidorActual) {
                        System.out.println("Sin respuesta del servidor de voz. Cambiando a "
                                + SERVIDORES[nuevoServidor][0] + ":" + SERVIDORES[nuevoServidor][1]);
                        servidorActual = nuevoServidor;
                        direccionServidor = InetAddress.getByName(SERVIDORES[servidorActual][0]);
                        puertoServidor = Integer.parseInt(SERVIDORES[servidorActual][1]);
                        ultimoPaqueteRecibido = ahora;
                    }
                }

                byte[] buffer = new byte[4096];
                microfono.read(buffer, 0, buffer.length);

                DatagramPacket paqueteEnvio = new DatagramPacket(
                        buffer, buffer.length, direccionServidor, puertoServidor);
                socket.send(paqueteEnvio);

                Thread.sleep(20);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
