package in.dreamlab.wicm.graph.computation;

import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.io.BooleanWritable;

import java.util.concurrent.atomic.AtomicBoolean;

public class GraphiteDebugWindowWorkerContext extends DefaultWorkerContext {
    protected static final String Init = "isInitialSuperstep";
    private static final String Fin = "finished";

    private int windowSuperstep;
    public AtomicBoolean finished = new AtomicBoolean();

    @Override
    public void preSuperstep() {
        finished.set(true);

        if(((BooleanWritable) getAggregatedValue(Init)).get())
            windowSuperstep = 0;
    }

    @Override
    public void postSuperstep() {
        windowSuperstep += 1;

        aggregate(Fin, new BooleanWritable(finished.get()));
    }

    public int getWindowSuperstep() {
        return windowSuperstep;
    }
}
