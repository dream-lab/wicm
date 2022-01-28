package in.dreamlab.wicm.algorithms.wicm_luds;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.graph.computation.DebugDeferredWindowIntervalComputation;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Time domain is UnsignedByte
 * MUST be used with GraphiteIntCustomWindowMaster
 * MUST be used with GraphiteDebugWindowWorkerContext
 */
public abstract class DebugUByteWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<UnsignedByte, S>, EP, E extends IntervalData<UnsignedByte, EP>, PW, P, IM extends IntervalMessage<UnsignedByte, P>> extends DebugDeferredWindowIntervalComputation<I, UnsignedByte, S, V, EP, E, PW, P, IM> {
    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";

    @Override
    public void preSuperstep() {
        super.preSuperstep();

        // get information from master regarding window execution
        isInitial = ((BooleanWritable) getAggregatedValue(Init)).get();
        windowInterval = new UByteInterval(((IntWritable) getAggregatedValue(WStart)).get(),
                ((IntWritable) getAggregatedValue(WEnd)).get());
        spareInterval = new UByteInterval(((IntWritable) getAggregatedValue(WEnd)).get(), ((IntWritable) getAggregatedValue(GEnd)).get());
    }
}

