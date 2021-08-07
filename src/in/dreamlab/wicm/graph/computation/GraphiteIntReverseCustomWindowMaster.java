package in.dreamlab.wicm.graph.computation;

import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

public class GraphiteIntReverseCustomWindowMaster extends DefaultMasterCompute {
    protected final Logger LOG = Logger.getLogger(GraphiteIntReverseCustomWindowMaster.class);

    private static final IntConfOption lowerEndpoint = new IntConfOption("lowerEndpoint", 0, "Lower Endpoint for graph");
    private static final IntConfOption upperEndpoint = new IntConfOption("upperEndpoint", 100, "Upper Endpoint for graph");
    private static final StrConfOption customWindows = new StrConfOption("windows", "", "Custom Windows for graph");

    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";
    private static final String GStart = "graphTimeStart";

    private static final String Fin = "finished";

    private long timedRegion;
    private int index;

    public void initialize() throws IllegalAccessException, InstantiationException {
        registerPersistentAggregator(Init, BooleanAndAggregator.class);
        registerPersistentAggregator(WStart, IntSumAggregator.class);
        registerPersistentAggregator(WEnd, IntSumAggregator.class);
        registerPersistentAggregator(GEnd, IntSumAggregator.class);
        registerPersistentAggregator(GStart, IntSumAggregator.class);

        registerAggregator(Fin, BooleanAndAggregator.class);

        timedRegion = 0;
        index = customWindows.get(getConf()).split(";").length-2;
    }

    public void compute() {
        timedRegion = System.nanoTime();
        setAggregatedValue(GStart, new IntWritable(lowerEndpoint.get(getConf())));
        setAggregatedValue(GEnd, new IntWritable(upperEndpoint.get(getConf())));
        String[] windowsMarkers = customWindows.get(getConf()).split(";");

        int start, end;
        if(getSuperstep() <= 0){ // initialise aggregators
            start = Integer.parseInt(windowsMarkers[index]);
            end = Integer.parseInt(windowsMarkers[index+1]);
            setAggregatedValue(WStart, new IntWritable(start));
            setAggregatedValue(WEnd, new IntWritable(end));
            setAggregatedValue(Init, new BooleanWritable(false));
            LOG.info("Window Start: " + start + ", Window End: " + end);
        } else { // for > 0 supersteps
            end = ((IntWritable) getAggregatedValue(WStart)).get();
            LOG.info(getSuperstep()+","+Fin+","+getAggregatedValue(Fin));

            if(((BooleanWritable) getAggregatedValue(Fin)).get()){
                index -= 1;
                if(index < 0){
                    LOG.info("Halting Computation..");
                    haltComputation();
                } else {
                    LOG.info("All messages exchanged. Updating lower and upper..");
                    start = Integer.parseInt(windowsMarkers[index]);

                    setAggregatedValue(WStart, new IntWritable(start));
                    setAggregatedValue(WEnd, new IntWritable(end));
                    setAggregatedValue(Init, new BooleanWritable(true));
                    LOG.info("Window Start: " + start + ", Window End: " + end);
                }
            } else {
                setAggregatedValue(Init, new BooleanWritable(false));
            }
        }
        timedRegion = System.nanoTime() - timedRegion;
        LOG.info("MasterComputeTime: "+timedRegion);
    }
}
