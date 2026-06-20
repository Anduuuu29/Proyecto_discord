import java.io.Serializable;

public class PaqueteDatos implements Serializable {

    private static final long serialVersionUID = 2L;

    private String tipo;
    private String emisor;
    private String mensaje;
    private byte[] datosVoz;
    private long timestamp;
    private String sessionId;
    private String servidorDestino;

    public PaqueteDatos(String tipo, String emisor, String mensaje, byte[] datos) {
        this.tipo = tipo;
        this.emisor = emisor;
        this.mensaje = mensaje;
        this.datosVoz = datos;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getEmisor() {
        return emisor;
    }

    public void setEmisor(String emisor) {
        this.emisor = emisor;
    }

    public String getMensaje() {
        return mensaje;
    }

    public void setMensaje(String mensaje) {
        this.mensaje = mensaje;
    }

    public byte[] getDatos() {
        return datosVoz;
    }

    public void setDatos(byte[] datos) {
        this.datosVoz = datos;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getServidorDestino() {
        return servidorDestino;
    }

    public void setServidorDestino(String servidorDestino) {
        this.servidorDestino = servidorDestino;
    }
}
