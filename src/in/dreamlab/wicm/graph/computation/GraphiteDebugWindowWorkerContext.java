package in.dreamlab.wicm.graph.computation;

import org.apache.giraph.worker.DefaultWorkerContext;
import org.apache.hadoop.io.BooleanWritable;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom worker context class to manage window shifting in WICM
 */
public class GraphiteDebugWindowWorkerContext extends DefaultWorkerContext {
    protected static final String Init = "isInitialSuperstep";
    private static final String Fin = "finished";

    private int windowSuperstep;
    // worker launches multiple threads which call compute in parallel across vertices
    // finished made atomic to avoid race conditions
    public AtomicBoolean finished = new AtomicBoolean();

    @Override
    public void preSuperstep() {
        finished.set(true);

        // local superstep counter for within a window
        if(((BooleanWritable) getAggregatedValue(Init)).get())
            windowSuperstep = 0;
    }

    @Override
    public void postSuperstep() {
        windowSuperstep += 1;
        // communicate to master whether the worker is finished
        aggregate(Fin, new BooleanWritable(finished.get()));
    }

    public int getWindowSuperstep() {
        return windowSuperstep;
    }
}
