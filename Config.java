public class Config {

    // --- Servidor de Texto ---
    public static final String HOST_PRIMARIO_TEXTO = "localhost";
    public static final int PUERTO_PRIMARIO_TEXTO = 5000;

    public static final String HOST_BACKUP_TEXTO = "localhost";
    public static final int PUERTO_BACKUP_TEXTO = 5001;

    // --- Servidor de Voz ---
    public static final String HOST_PRIMARIO_VOZ = "localhost";
    public static final int PUERTO_PRIMARIO_VOZ = 6000;

    public static final String HOST_BACKUP_VOZ = "localhost";
    public static final int PUERTO_BACKUP_VOZ = 6001;

    // --- Timeouts ---
    public static final long TIEMPO_RECONEXION_MS = 2000;
    public static final long TIMEOUT_VOZ_MS = 1000;
    public static final int MAX_HISTORIAL_CHAT = 100;
}
