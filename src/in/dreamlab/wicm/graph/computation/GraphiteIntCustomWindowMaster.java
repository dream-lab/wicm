package in.dreamlab.wicm.graph.computation;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

/**
 * Custom master compute class for coordination with workers
 * Master decides when to shift windows through data sharing with workers
 * This class is used for temporal algorithms with move forward with time
 */
public class GraphiteIntCustomWindowMaster extends DefaultMasterCompute {
    protected final Logger LOG = Logger.getLogger(GraphiteIntCustomWindowMaster.class);

    // Graph lifespan is [lowerEndpoint, upperEndpoint)
    private static final IntConfOption lowerEndpoint = new IntConfOption("lowerEndpoint", 0, "Lower Endpoint for graph");
    private static final IntConfOption upperEndpoint = new IntConfOption("upperEndpoint", 100, "Upper Endpoint for graph");

    // User feeds temporal partitions for WICM
    private static final StrConfOption customWindows = new StrConfOption("windows", "", "Custom Windows for graph");

    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";

    private static final String Fin = "finished";

    private long timedRegion;
    private int index;

    public void initialize() throws IllegalAccessException, InstantiationException {
        // master communicates important information like current window lifespan,
        // is superstep the first superstep for window execution, etc.
        // through aggregators.
        registerPersistentAggregator(Init, BooleanAndAggregator.class);
        registerPersistentAggregator(WStart, IntSumAggregator.class);
        registerPersistentAggregator(WEnd, IntSumAggregator.class);
        registerPersistentAggregator(GEnd, IntSumAggregator.class);

        registerAggregator(Fin, BooleanAndAggregator.class);

        timedRegion = 0;
        index = 0;
    }

    public void compute() {
        // start from the first window, then shift towards later windows
        timedRegion = System.nanoTime();
        setAggregatedValue(GEnd, new IntWritable(upperEndpoint.get(getConf())));
        String[] windowsMarkers = customWindows.get(getConf()).split(";");

        int start, end;
        if(getSuperstep() <= 0){
            start = Integer.parseInt(windowsMarkers[index]); // initialise current window: [start, end)
            end = Integer.parseInt(windowsMarkers[index+1]);
            setAggregatedValue(WStart, new IntWritable(start)); // communicate with workers
            setAggregatedValue(WEnd, new IntWritable(end));
            setAggregatedValue(Init, new BooleanWritable(false));
            LOG.info("Window Start: " + start + ", Window End: " + end);
        } else { // for > 0 supersteps
            start = ((IntWritable) getAggregatedValue(WEnd)).get();
            LOG.info(getSuperstep()+","+Fin+","+getAggregatedValue(Fin));

            if(((BooleanWritable) getAggregatedValue(Fin)).get()){ // all workers finished
                index += 1;
                if(start >= upperEndpoint.get(getConf())){ // all windows processed, halt!
                    LOG.info("Halting Computation..");
                    haltComputation();
                } else {
                    LOG.info("All messages exchanged. Updating lower and upper..");
                    end = Integer.parseInt(windowsMarkers[index+1]); // shift to next window

                    setAggregatedValue(WStart, new IntWritable(start)); // communicate with workers
                    setAggregatedValue(WEnd, new IntWritable(end));
                    setAggregatedValue(Init, new BooleanWritable(true));
                    LOG.info("Window Start: " + start + ", Window End: " + end);
                }
            } else { // some worker is not finished, continue window execution
                setAggregatedValue(Init, new BooleanWritable(false));
            }
        }
        timedRegion = System.nanoTime() - timedRegion;
        LOG.info("MasterComputeTime: "+timedRegion);
    }
}
