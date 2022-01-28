package in.dreamlab.wicm.graph.computation;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public abstract class DebugUByteWindowIntervalComputation<I extends WritableComparable, S, V extends IntervalData<UnsignedByte, S>, EP, E extends IntervalData<UnsignedByte, EP>, PW, P, IM extends IntervalMessage<UnsignedByte, P>> extends DebugDeferredWindowIntervalComputation<I, UnsignedByte, S, V, EP, E, PW, P, IM> {
    private static final String Init = "isInitialSuperstep";
    private static final String WStart = "windowTimeStart";
    private static final String WEnd = "windowTimeEnd";
    private static final String GEnd = "graphTimeEnd";

    @Override
    public void preSuperstep() {
        super.preSuperstep();

        isInitial = ((BooleanWritable) getAggregatedValue(Init)).get();
        windowInterval = new UByteInterval(((IntWritable) getAggregatedValue(WStart)).get(),
                ((IntWritable) getAggregatedValue(WEnd)).get());
        spareInterval = new UByteInterval(((IntWritable) getAggregatedValue(WEnd)).get(), ((IntWritable) getAggregatedValue(GEnd)).get());
    }
}

