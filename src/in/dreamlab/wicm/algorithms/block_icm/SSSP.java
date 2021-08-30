package in.dreamlab.wicm.algorithms.block_icm;

import in.dreamlab.graphite.comm.messages.IntDoubleIntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntDoubleIntervalData;
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

public class SSSP extends
        DebugBlockWarpBasicIntervalComputation<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, Double, Double, IntDoubleIntervalMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final int travelTime = 1;
    public static final double travelDistance = 1.;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, Double, Double, IntDoubleIntervalMessage> intervalVertex) {
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), 0.0);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Double.POSITIVE_INFINITY);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Double>> compute(
            IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, Double, Double, IntDoubleIntervalMessage> intervalVertex,
            Interval<Integer> interval, Double currentDistance, Double candidateDistance) throws IOException {
        if (currentDistance > candidateDistance) {
            intervalVertex.setState(interval, candidateDistance);
            return Collections.singleton(new ImmutablePair<>(interval, candidateDistance));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<IntDoubleIntervalMessage> scatter(
            IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, Double, Double, IntDoubleIntervalMessage> intervalVertex,
            Edge<IntWritable, IntDoubleIntervalData> edge, Interval<Integer> interval, Double vState,
            Double nullProperty) {
        return Collections.singleton(new IntDoubleIntervalMessage(
                new IntInterval(interval.getStart() + travelTime, Integer.MAX_VALUE),
                vState + travelDistance));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

}
