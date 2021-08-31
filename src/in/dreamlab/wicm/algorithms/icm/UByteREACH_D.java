package in.dreamlab.wicm.algorithms.icm;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.UByteBooleanStartSlimMessage;
import in.dreamlab.wicm.graph.computation.DebugBasicIntervalComputation;
import in.dreamlab.wicm.graphData.UByteBooleanIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class UByteREACH_D extends
        DebugBasicIntervalComputation<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, Boolean, Boolean, UByteBooleanStartSlimMessage> {

    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "Reachability Source Vertex ID");
    public static final int travelTime = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, Boolean, Boolean, UByteBooleanStartSlimMessage> intervalVertex) {
        intervalVertex.getValue().getPropertyMap().clear();
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), true);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), false);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<UnsignedByte>, Boolean>> compute(
            IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, Boolean, Boolean, UByteBooleanStartSlimMessage> intervalVertex,
            Interval<UnsignedByte> interval, Boolean currentReachabilityState, Boolean candidateReachabilityState) throws IOException {
        if (!currentReachabilityState) {
            intervalVertex.setState(interval, candidateReachabilityState);
            return Collections.singleton(new ImmutablePair<>(interval, candidateReachabilityState));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<UByteBooleanStartSlimMessage> scatter(
            IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, Boolean, Boolean, UByteBooleanStartSlimMessage> intervalVertex,
            Edge<IntWritable, UByteBooleanIntervalData> edge, Interval<UnsignedByte> interval, Boolean reachabilityState,
            Boolean nullProperty) {
        Integer reachingTime = interval.getStart().intValue() + travelTime;
        return Collections.singleton(new UByteBooleanStartSlimMessage(reachingTime, true));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }
}
