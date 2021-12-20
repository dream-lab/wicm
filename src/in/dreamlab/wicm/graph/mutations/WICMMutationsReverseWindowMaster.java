package in.dreamlab.wicm.graph.mutations;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

public class WICMMutationsReverseWindowMaster extends WICMMutationsWindowMaster {

    public void initialize() throws IllegalAccessException, InstantiationException {
        super.initialize();
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
            setAggregatedValue(Mutation, new BooleanWritable(false));
            LOG.info("Window Start: " + start + ", Window End: " + end);
        } else { // for > 0 supersteps
            end = ((IntWritable) getAggregatedValue(WStart)).get();
            LOG.info(getSuperstep()+","+Fin+","+getAggregatedValue(Fin));

            if(((BooleanWritable) getAggregatedValue(Fin)).get()) {
                if (end <= lowerEndpoint.get(getConf())) {
                    LOG.info("Halting Computation..");
                    haltComputation();
                } else {
                    if (((BooleanWritable) getAggregatedValue(Mutation)).get()) {
                        index -= 1;
                        setAggregatedValue(Mutation, new BooleanWritable(false));
                        start = Integer.parseInt(windowsMarkers[index]);
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
