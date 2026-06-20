import java.io.Serializable;

public class RaftLogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    public int index;
    public int term;
    public PaqueteDatos comando;

    public RaftLogEntry(int index, int term, PaqueteDatos comando) {
        this.index = index;
        this.term = term;
        this.comando = comando;
    }
}
