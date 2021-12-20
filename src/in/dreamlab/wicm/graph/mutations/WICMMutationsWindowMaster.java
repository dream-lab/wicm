package in.dreamlab.wicm.graph.mutations;

import in.dreamlab.wicm.graph.computation.GraphiteIntCustomWindowMaster;
import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.StrConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

public class WICMMutationsWindowMaster extends DefaultMasterCompute {
    protected final Logger LOG = Logger.getLogger(GraphiteIntCustomWindowMaster.class);

    static final IntConfOption lowerEndpoint = new IntConfOption("lowerEndpoint", 0, "Lower Endpoint for graph");
    static final IntConfOption upperEndpoint = new IntConfOption("upperEndpoint", 100, "Upper Endpoint for graph");
    static final StrConfOption customWindows = new StrConfOption("windows", "", "Custom Windows for graph");

    static final String Init = "isInitialSuperstep";
    static final String WStart = "windowTimeStart";
    static final String WEnd = "windowTimeEnd";
    static final String GStart = "graphTimeStart";
    static final String GEnd = "graphTimeEnd";

    static final String Mutation = "isMutationSuperstep";
    static final String WindowNum = "windowNumber";

    static final String Fin = "finished";

    long timedRegion;
    int index;
    int windowNum;

    public void initialize() throws IllegalAccessException, InstantiationException {
        registerPersistentAggregator(Init, BooleanAndAggregator.class);
        registerPersistentAggregator(GStart, IntSumAggregator.class);
        registerPersistentAggregator(GEnd, IntSumAggregator.class);
        registerPersistentAggregator(WStart, IntSumAggregator.class);
        registerPersistentAggregator(WEnd, IntSumAggregator.class);

        registerPersistentAggregator(Mutation, BooleanAndAggregator.class);
        registerPersistentAggregator(WindowNum, IntSumAggregator.class);

        registerAggregator(Fin, BooleanAndAggregator.class);

        timedRegion = 0;
        index = 0;
        windowNum = 1;
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
            setAggregatedValue(Mutation, new BooleanWritable(false));
            LOG.info("Window Start: " + start + ", Window End: " + end);
        } else { // for > 0 supersteps
            start = ((IntWritable) getAggregatedValue(WEnd)).get();
            LOG.info(getSuperstep()+","+Fin+","+getAggregatedValue(Fin));

            if(((BooleanWritable) getAggregatedValue(Fin)).get()){
                if(start >= upperEndpoint.get(getConf())){
                    LOG.info("Halting Computation..");
                    haltComputation();
                } else {
                    if(((BooleanWritable) getAggregatedValue(Mutation)).get()) {
                        index += 1;
                        setAggregatedValue(Mutation, new BooleanWritable(false));
                        end = Integer.parseInt(windowsMarkers[index+1]);
                        setAggregatedValue(WStart, new IntWritable(start));
                        setAggregatedValue(WEnd, new IntWritable(end));
                        setAggregatedValue(Init, new BooleanWritable(true));
                        LOG.info("Updating window: Window Start: " + start + ", Window End: " + end);
                    } else {
                        windowNum++;
                        LOG.info("All messages exchanged. Reading next mutation file..");
                        setAggregatedValue(Mutation, new BooleanWritable(true));
                        setAggregatedValue(WindowNum, new IntWritable(windowNum));
                    }
                }
            } else {
                setAggregatedValue(Init, new BooleanWritable(false));
                setAggregatedValue(Mutation, new BooleanWritable(false));
            }
        }
        timedRegion = System.nanoTime() - timedRegion;
        LOG.info("MasterComputeTime: "+timedRegion);
    }
}
