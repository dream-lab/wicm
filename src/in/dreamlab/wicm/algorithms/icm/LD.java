package in.dreamlab.wicm.algorithms.icm;

import in.dreamlab.graphite.comm.messages.IntIntIntervalMessage;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.graph.computation.DebugBasicIntervalComputation;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.Algorithm;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

@Algorithm(name = "Time-Dependent Latest Departure Time")
public class LD extends
        DebugBasicIntervalComputation<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> {

    public static final IntConfOption DESTINATION_ID = new IntConfOption("sourceId", 1, "LD Destination ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex) {
        if (intervalVertex.getId().get() == DESTINATION_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), intervalVertex.getLifespan().getEnd() - 1);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), -1);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Integer>> compute(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex,
            Interval<Integer> interval, Integer currentLatestDepartureTime, Integer candidateLatestDepartureTime)
            throws IOException {
        if (currentLatestDepartureTime < candidateLatestDepartureTime) {
            intervalVertex.setState(interval, candidateLatestDepartureTime);
            return Collections
                    .singleton(new ImmutablePair<>(interval, candidateLatestDepartureTime));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<IntIntIntervalMessage> scatter(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntIntIntervalMessage> intervalVertex,
            Edge<IntWritable, IntIntIntervalData> edge, Interval<Integer> interval, Integer latestDepartureTime,
            Integer nullProperty) {
        int departureTime = latestDepartureTime - travelDuration;
        if (departureTime >= interval.getStart()) {
            if (departureTime < interval.getEnd()) {
                /* Take the latest (largest) time-point which ensures time constraint is meet */
                return Collections.singleton(new IntIntIntervalMessage(new IntInterval(0, departureTime + 1),departureTime));
            } else {
                /* You must take the in-edge while its valid */
                return Collections.singleton(new IntIntIntervalMessage(new IntInterval(0, interval.getEnd()), interval.getEnd() - 1));
            }
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }
}
