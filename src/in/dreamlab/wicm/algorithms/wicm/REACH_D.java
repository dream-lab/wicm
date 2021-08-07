package in.dreamlab.wicm.algorithms.wicm;

import in.dreamlab.graphite.comm.messages.IntBooleanIntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntBooleanIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.graph.computation.DebugIntWindowIntervalComputation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

@Algorithm(name = "Temporal Reachability")
public class REACH_D extends
        DebugIntWindowIntervalComputation<IntWritable, Boolean, IntBooleanIntervalData, Boolean, IntBooleanIntervalData, Boolean, Boolean, IntBooleanIntervalMessage> {

    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "Reachability Source Vertex ID");
    public static final int travelTime = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Boolean, IntBooleanIntervalData, Boolean, Boolean, IntBooleanIntervalMessage> intervalVertex) {
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), true);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), false);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Boolean>> compute(
            IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Boolean, IntBooleanIntervalData, Boolean, Boolean, IntBooleanIntervalMessage> intervalVertex,
            Interval<Integer> interval, Boolean currentReachabilityState, Boolean candidateReachabilityState) throws IOException {
        if (!currentReachabilityState) {
            intervalVertex.setState(interval, candidateReachabilityState);
            return Collections.singleton(new ImmutablePair<>(interval, candidateReachabilityState));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<IntBooleanIntervalMessage> scatter(
            IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Boolean, IntBooleanIntervalData, Boolean, Boolean, IntBooleanIntervalMessage> intervalVertex,
            Edge<IntWritable, IntBooleanIntervalData> edge, Interval<Integer> interval, Boolean reachabilityState,
            Boolean nullProperty) {
        Integer reachingTime = interval.getStart() + travelTime;
        return Collections.singleton(new IntBooleanIntervalMessage(new IntInterval(reachingTime, Integer.MAX_VALUE), true));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

    @Override
    public boolean isDefault(Boolean vertexValue) {
        return !vertexValue;
    }
}
