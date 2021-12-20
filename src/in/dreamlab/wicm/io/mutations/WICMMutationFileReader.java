package in.dreamlab.wicm.io.mutations;

import org.apache.giraph.conf.GiraphConfigurationSettable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.regex.Pattern;

public abstract class WICMMutationFileReader<I extends WritableComparable,
        V extends Writable, E extends Writable> implements GiraphConfigurationSettable<I,V,E> {
    protected static Logger LOG = Logger.getLogger(WICMMutationFileReader.class);
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    public enum MODE {
        ADD_VERTEX,
        TRUNCATE_VERTEX,
        DELETE_VERTEX,
        REPLACE_EDGE
    };

    private I vertexId = null;
    private V vertexValue = null;
    private List<Edge<I,E>> edges = null;
    private MODE mode = MODE.DELETE_VERTEX;

    ImmutableClassesGiraphConfiguration<I,V,E> conf = null;
    BufferedReader br = null;
    boolean hasNext = false;

    public WICMMutationFileReader() {
    }

    public ImmutableClassesGiraphConfiguration<I,V,E> getConf() {
        return conf;
    }

    public void initialise(String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(getConf());
        Path fPath = new Path(path);
        if (!fileSystem.exists(fPath)) {
            LOG.info("File does not exists");
            return;
        }

        br = new BufferedReader(new InputStreamReader(fileSystem.open(fPath)));
    }

    public boolean hasNext() throws IOException {
        if(br == null)
            return false;

        next();
        return hasNext;
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> immutableClassesGiraphConfiguration) {
        this.conf = immutableClassesGiraphConfiguration;
    }

    public void close() throws IOException {
        if(br != null) {
            br.close();
            br = null;
        }
    }

    void next() throws IOException {
        String line;
        line = br.readLine();
        if(line == null) {
            hasNext = false;
            vertexId = null;
            vertexValue = null;
            edges = null;
            mode = MODE.DELETE_VERTEX;
        } else {
            String[] tokens = tokenise(line);
            mode = setMode(tokens);
            vertexId = setVertexId(tokens);
            vertexValue = setVertexValue(tokens);
            edges = setEdges(tokens);
            hasNext = true;
        }
    }

    MODE setMode(String[] line) {
        if(line[0].equals("ADD")) return MODE.ADD_VERTEX;
        if(line[0].equals("TRUNCATE")) return MODE.TRUNCATE_VERTEX;
        if(line[0].equals("DELETE")) return MODE.DELETE_VERTEX;
        if(line[0].equals("EDGE")) return MODE.REPLACE_EDGE;
        return MODE.DELETE_VERTEX;
    }

    String[] tokenise(String line) {
        return SEPARATOR.split(line);
    }

    abstract I setVertexId(String[] line);
    abstract V setVertexValue(String[] line);
    abstract List<Edge<I,E>> setEdges(String[] line);

    public I getVertexId() {
        return vertexId;
    }

    public List<Edge<I, E>> getEdges() {
        return edges;
    }

    public V getVertexValue() {
        return vertexValue;
    }

    public MODE getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "WICMMutationFileReader{" +
                "vertexId=" + vertexId.toString() +
                ", vertexValue=" + vertexValue.toString() +
                ", edges=" + edges.toString() +
                ", mode=" + mode.toString() +
                '}';
    }
}
