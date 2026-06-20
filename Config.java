public class Config {

    // --- Gateway ---
    public static final String HOST_GATEWAY_PRIMARIO = "localhost";
    public static final int PUERTO_GATEWAY = 4999;

    public static final String HOST_GATEWAY_BACKUP = "localhost";
    public static final int PUERTO_GATEWAY_BACKUP = 4998;

    // --- Servidores Principales (candidatos para seleccion por ping) ---
    public static final String HOST_PRINCIPAL_PRIMARIO = "localhost";
    public static final int PUERTO_PRINCIPAL_PRIMARIO = 5200;

    public static final String HOST_PRINCIPAL_BACKUP = "localhost";
    public static final int PUERTO_PRINCIPAL_BACKUP = 5201;

    public static final String[][] SERVIDORES_PRINCIPALES = {
        {"localhost", "5200", "Principal primario"},
        {"localhost", "5201", "Principal backup"},
        {"localhost", "5202", "Principal lejano"}
    };

    public static final long TIMEOUT_PING_MS = 300;

    // --- Servidor de Chat ---
    public static final String HOST_TEXTO_PRIMARIO = "localhost";
    public static final int PUERTO_TEXTO_PRIMARIO = 5000;

    public static final String HOST_TEXTO_BACKUP = "localhost";
    public static final int PUERTO_TEXTO_BACKUP = 5001;

    // --- Servidor de Voz ---
    public static final String HOST_VOZ_PRIMARIO = "localhost";
    public static final int PUERTO_VOZ_PRIMARIO = 6000;

    public static final String HOST_VOZ_BACKUP = "localhost";
    public static final int PUERTO_VOZ_BACKUP = 6001;

    // --- Timeouts ---
    public static final long TIEMPO_RECONEXION_MS = 2000;
    public static final long TIMEOUT_VOZ_MS = 600;
    public static final int MAX_HISTORIAL_CHAT = 100;
    public static final long HEARTBEAT_INTERVALO_MS = 1000;
}
