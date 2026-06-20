import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;

public class LogEvento implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<LogEntry> entradas = Collections.synchronizedList(new ArrayList<>());
    private static final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");

    public void registrar(int lamport, String nodo, String tipo, String detalle) {
        entradas.add(new LogEntry(lamport, System.currentTimeMillis(), nodo, tipo, detalle));
    }

    public String imprimir() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n=== LOG DE EVENTOS (Lamport) ===\n");
        sb.append(String.format("%-9s %-10s %-22s %s%n", "Lamport", "Hora", "Nodo", "Evento"));
        sb.append("------------------------------------------------------------\n");
        synchronized (entradas) {
            for (LogEntry e : entradas) {
                sb.append(String.format("L%-8d %-10s %-22s %s | %s%n",
                    e.lamport, sdf.format(new Date(e.timestampFisico)),
                    e.nodo, e.tipo, e.detalle));
            }
        }
        sb.append("============================================================\n");
        sb.append("Total eventos: " + entradas.size() + "\n");
        return sb.toString();
    }

    public int size() {
        return entradas.size();
    }

    public static class LogEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        int lamport;
        long timestampFisico;
        String nodo;
        String tipo;
        String detalle;

        public LogEntry(int lamport, long timestampFisico, String nodo, String tipo, String detalle) {
            this.lamport = lamport;
            this.timestampFisico = timestampFisico;
            this.nodo = nodo;
            this.tipo = tipo;
            this.detalle = detalle;
        }
    }
}
