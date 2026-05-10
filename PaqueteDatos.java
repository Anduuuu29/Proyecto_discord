import java.io.Serializable;

public class PaqueteDatos implements Serializable {

    private static final long serialVersionUID = 1L;

    private String tipo;
    private String emisor;
    private String mensaje;
    private byte[] datosVoz;
    private long timestamp; // Asignado por el Servidor al procesar el paquete (no por el cliente)

    public PaqueteDatos(String tipo, String emisor, String mensaje, byte[] datos) {
        this.tipo = tipo;
        this.emisor = emisor;
        this.mensaje = mensaje;
        this.datosVoz = datos;
    }

    public String getTipo() {
        return tipo;
    }

    public String getEmisor() {
        return emisor;
    }

    public String getMensaje() {
        return mensaje;
    }

    public byte[] getDatos() {
        return datosVoz;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public void setEmisor(String emisor) {
        this.emisor = emisor;
    }

    public void setMensaje(String mensaje) {
        this.mensaje = mensaje;
    }

    public void setDatos(byte[] datos) {
        this.datosVoz = datos;
    }

}