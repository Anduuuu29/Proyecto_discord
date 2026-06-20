import java.io.Serializable;

public class PaqueteDatos implements Serializable {

    private static final long serialVersionUID = 2L;

    private String tipo;
    private String emisor;
    private String mensaje;
    private byte[] datosVoz;
    private long timestamp;
    private int relojLamport;
    private String sessionId;
    private String servidorDestino;

    // Raft
    private RaftLogEntry[] raftEntries;
    private int raftTerm;
    private int raftPrevLogIndex = -1;
    private int raftPrevLogTerm;
    private int raftLeaderCommit = -1;
    private boolean raftSuccess;

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

    public int getRelojLamport() {
        return relojLamport;
    }

    public void setRelojLamport(int relojLamport) {
        this.relojLamport = relojLamport;
    }

    public RaftLogEntry[] getRaftEntries() {
        return raftEntries;
    }

    public void setRaftEntries(RaftLogEntry[] raftEntries) {
        this.raftEntries = raftEntries;
    }

    public int getRaftTerm() {
        return raftTerm;
    }

    public void setRaftTerm(int raftTerm) {
        this.raftTerm = raftTerm;
    }

    public int getRaftPrevLogIndex() {
        return raftPrevLogIndex;
    }

    public void setRaftPrevLogIndex(int raftPrevLogIndex) {
        this.raftPrevLogIndex = raftPrevLogIndex;
    }

    public int getRaftPrevLogTerm() {
        return raftPrevLogTerm;
    }

    public void setRaftPrevLogTerm(int raftPrevLogTerm) {
        this.raftPrevLogTerm = raftPrevLogTerm;
    }

    public int getRaftLeaderCommit() {
        return raftLeaderCommit;
    }

    public void setRaftLeaderCommit(int raftLeaderCommit) {
        this.raftLeaderCommit = raftLeaderCommit;
    }

    public boolean isRaftSuccess() {
        return raftSuccess;
    }

    public void setRaftSuccess(boolean raftSuccess) {
        this.raftSuccess = raftSuccess;
    }
}
