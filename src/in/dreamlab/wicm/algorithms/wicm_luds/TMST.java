package in.dreamlab.wicm.algorithms.wicm_luds;

import in.dreamlab.graphite.comm.messages.IntPairIntIntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.graphData.IntPairIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class TMST extends
        DebugIntWindowIntervalComputation<IntWritable, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, Pair<Integer, Integer>, Pair<Integer, Integer>, IntPairIntIntervalMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, Pair<Integer, Integer>, Pair<Integer, Integer>, IntPairIntIntervalMessage> intervalVertex) {
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(intervalVertex.getId().get(), 0));
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(-1, Integer.MAX_VALUE));
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Pair<Integer,Integer>>> compute(
            IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, Pair<Integer, Integer>, Pair<Integer, Integer>, IntPairIntIntervalMessage> intervalVertex,
            Interval<Integer> interval, Pair<Integer, Integer> currentParent, Pair<Integer, Integer> candidateParent) throws IOException {
        if (candidateParent.getRight() < currentParent.getRight()) {
            intervalVertex.setState(interval, candidateParent);
            return Collections.singleton(new ImmutablePair<>(interval, candidateParent));
        } else if(candidateParent.getRight().equals(currentParent.getRight())){
            if(candidateParent.getLeft() < currentParent.getLeft())
                intervalVertex.setState(interval,candidateParent);
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<IntPairIntIntervalMessage> scatter(
            IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, Pair<Integer, Integer>, Pair<Integer, Integer>, IntPairIntIntervalMessage> intervalVertex,
            Edge<IntWritable, IntIntIntervalData> edge, Interval<Integer> interval, Pair<Integer,Integer> currentParent, Integer nullProperty) {
        int reachingTime = Integer.max(currentParent.getRight(), interval.getStart()) + travelDuration;
        return Collections.singleton(new IntPairIntIntervalMessage(
                new IntInterval(reachingTime, Integer.MAX_VALUE), new MutablePair<>(intervalVertex.getId().get(), reachingTime)));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

    @Override
    public boolean isDefault(Pair<Integer, Integer> vertexValue) {
        return vertexValue.getRight() == Integer.MAX_VALUE;
    }
}
