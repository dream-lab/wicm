package in.dreamlab.wicm.algorithms.icm_luds;

import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.graph.computation.DebugBlockWarpBasicIntervalComputation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class EAT extends
        DebugBlockWarpBasicIntervalComputation<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex) {
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), 0);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Integer.MAX_VALUE);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Integer>> compute(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex,
            Interval<Integer> interval, Integer currentEarliestArrival, Integer candidateEarliestArrival) throws IOException {
        if (currentEarliestArrival > candidateEarliestArrival) {
            intervalVertex.setState(interval, candidateEarliestArrival);
            return Collections.singleton(new ImmutablePair<>(interval, candidateEarliestArrival));
        }

        return Collections.emptySet();
    }

    @Override
    public Iterable<IntIntIntervalMessage> scatter(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex,
            Edge<IntWritable, IntIntIntervalData> edge, Interval<Integer> interval, Integer earliestArrival, Integer nullProperty) {
        Integer reachingTime = Integer.max(earliestArrival, interval.getStart()) + travelDuration;
        return Collections.singleton(new IntIntIntervalMessage(new IntInterval(reachingTime, Integer.MAX_VALUE), reachingTime));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }
}
