package in.dreamlab.wicm.algorithms.icm;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.UByteIntStartSlimMessage;
import in.dreamlab.wicm.graph.computation.DebugBasicIntervalComputation;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class UByteEAT extends
        DebugBasicIntervalComputation<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> intervalVertex) {
        intervalVertex.getValue().getPropertyMap().clear();
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), 0);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Integer.MAX_VALUE);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<UnsignedByte>, Integer>> compute(
            IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> intervalVertex,
            Interval<UnsignedByte> interval, Integer currentEarliestArrival, Integer candidateEarliestArrival) throws IOException {
        if (currentEarliestArrival > candidateEarliestArrival) {
            intervalVertex.setState(interval, candidateEarliestArrival);
            return Collections.singleton(new ImmutablePair<>(interval, candidateEarliestArrival));
        }

        return Collections.emptySet();
    }

    @Override
    public Iterable<UByteIntStartSlimMessage> scatter(
            IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> intervalVertex,
            Edge<IntWritable, UByteIntIntervalData> edge, Interval<UnsignedByte> interval, Integer earliestArrival, Integer nullProperty) {
        int reachingTime = Integer.max(earliestArrival, interval.getStart().intValue()) + travelDuration;
        return Collections.singleton(new UByteIntStartSlimMessage(new UnsignedByte(reachingTime), reachingTime));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }
}
