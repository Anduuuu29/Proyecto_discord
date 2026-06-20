import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import javax.sound.sampled.*;

public class ClienteVoz {

    private static volatile long ultimoPaqueteRecibido = System.currentTimeMillis();
    private static volatile String direccionHost;
    private static volatile int puertoHost;
    private static volatile boolean cambioABackup = false;

    private static String primaryHost;
    private static int primaryPuerto;
    private static String backupHost;
    private static int backupPuerto;

    public static void main(String[] args) {
        primaryHost = Config.HOST_VOZ_PRIMARIO;
        primaryPuerto = Config.PUERTO_VOZ_PRIMARIO;
        backupHost = Config.HOST_VOZ_BACKUP;
        backupPuerto = Config.PUERTO_VOZ_BACKUP;

        if (args.length >= 2) {
            primaryHost = args[0];
            primaryPuerto = Integer.parseInt(args[1]);
        }

        direccionHost = primaryHost;
        puertoHost = primaryPuerto;

        System.out.println("Cliente de voz iniciado en " + direccionHost + ":" + puertoHost);

        try {
            DatagramSocket socket = new DatagramSocket();
            AudioFormat formato = new AudioFormat(
                    16000.0f, 16, 1, true, false
            );

            TargetDataLine microfono = AudioSystem.getTargetDataLine(formato);
            microfono.open(formato);
            microfono.start();

            SourceDataLine parlantes = AudioSystem.getSourceDataLine(formato);
            parlantes.open(formato);
            parlantes.start();

            // Hilo de recepcion de audio
            Thread hiloRecepcion = new Thread(() -> {
                try {
                    byte[] bufferRecepcion = new byte[16384];
                    while (true) {
                        DatagramPacket paqueteRecibido = new DatagramPacket(
                                bufferRecepcion, bufferRecepcion.length);
                        socket.receive(paqueteRecibido);
                        byte[] datos = paqueteRecibido.getData();
                        if (paqueteRecibido.getLength() == 1 && datos[0] == 0x02) {
                            ultimoPaqueteRecibido = System.currentTimeMillis();
                            continue;
                        }
                        parlantes.write(datos, 0, paqueteRecibido.getLength());
                        ultimoPaqueteRecibido = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    System.out.println("Error en recepcion de audio.");
                }
            });
            hiloRecepcion.start();

            // Hilo de monitoreo: verifica si el primario volvio (solo cuando estamos en backup)
            Thread monitorPrimario = new Thread(() -> {
                while (true) {
                    try { Thread.sleep(2000); } catch (InterruptedException e) { break; }
                    if (!cambioABackup) continue;

                    try (DatagramSocket s = new DatagramSocket()) {
                        InetAddress addr = InetAddress.getByName(primaryHost);
                        byte[] ping = { 0x01 };
                        s.send(new DatagramPacket(ping, ping.length, addr, primaryPuerto));
                        s.setSoTimeout(500);
                        DatagramPacket resp = new DatagramPacket(new byte[1], 1);
                        s.receive(resp);
                        if (resp.getAddress().equals(addr) && resp.getPort() == primaryPuerto) {
                            System.out.println("Servidor primario recuperado. Volviendo a "
                                    + primaryHost + ":" + primaryPuerto);
                            direccionHost = primaryHost;
                            puertoHost = primaryPuerto;
                            cambioABackup = false;
                            ultimoPaqueteRecibido = System.currentTimeMillis();
                        }
                    } catch (Exception e) {
                        // Primario aun caido, seguimos en backup
                    }
                }
            });
            monitorPrimario.setDaemon(true);
            monitorPrimario.start();

            // Bucle principal: enviar audio + PING + failover
            long ultimoPing = 0;

            while (true) {
                long ahora = System.currentTimeMillis();

                // PING al servidor actual cada 500ms
                if (ahora - ultimoPing > 500) {
                    byte[] ping = { 0x01 };
                    DatagramPacket paquetePing = new DatagramPacket(
                            ping, ping.length,
                            InetAddress.getByName(direccionHost), puertoHost);
                    socket.send(paquetePing);
                    ultimoPing = ahora;
                }

                // Failover: solo si estamos en primario y no responde
                if (!cambioABackup && ahora - ultimoPaqueteRecibido > Config.TIMEOUT_VOZ_MS) {
                    System.out.println("Servidor primario sin respuesta. Cambiando a backup "
                            + backupHost + ":" + backupPuerto);
                    direccionHost = backupHost;
                    puertoHost = backupPuerto;
                    cambioABackup = true;
                    ultimoPaqueteRecibido = ahora;
                }

                // Enviar audio
                byte[] buffer = new byte[4096];
                microfono.read(buffer, 0, buffer.length);

                DatagramPacket paqueteEnvio = new DatagramPacket(
                        buffer, buffer.length,
                        InetAddress.getByName(direccionHost), puertoHost);
                socket.send(paqueteEnvio);

                Thread.sleep(20);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
