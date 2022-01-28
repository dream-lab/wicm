package in.dreamlab.wicm.algorithms.wicm_luds;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.UByteIntEndSlimMessage;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
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
public class UByteLD extends
        DebugUByteReverseWindowIntervalComputation<IntWritable, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntEndSlimMessage> {

    public static final IntConfOption DESTINATION_ID = new IntConfOption("sourceId", 1, "LD Destination ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntEndSlimMessage> intervalVertex) {
        intervalVertex.getValue().getPropertyMap().clear();
        if (intervalVertex.getId().get() == DESTINATION_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), intervalVertex.getLifespan().getEnd().intValue() - 1);
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), -1);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<UnsignedByte>, Integer>> compute(
            IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntEndSlimMessage> intervalVertex,
            Interval<UnsignedByte> interval, Integer currentLatestDepartureTime, Integer candidateLatestDepartureTime)
            throws IOException {
        if (currentLatestDepartureTime < candidateLatestDepartureTime) {
            intervalVertex.setState(interval, candidateLatestDepartureTime);
            return Collections
                    .singleton(new ImmutablePair<>(interval, candidateLatestDepartureTime));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<UByteIntEndSlimMessage> scatter(
            IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntEndSlimMessage> intervalVertex,
            Edge<IntWritable, UByteIntIntervalData> edge, Interval<UnsignedByte> interval, Integer latestDepartureTime,
            Integer nullProperty) {
        int departureTime = latestDepartureTime - travelDuration;
        if (departureTime >= interval.getStart().intValue()) {
            if (departureTime < interval.getEnd().intValue()) {
                /* Take the latest (largest) time-point which ensures time constraint is meet */
                return Collections.singleton(new UByteIntEndSlimMessage(departureTime+1,departureTime));
            } else {
                /* You must take the in-edge while its valid */
                return Collections.singleton(new UByteIntEndSlimMessage(interval.getEnd().intValue(), interval.getEnd().intValue()-1));
            }
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

    @Override
    public boolean isDefault(Integer vertexValue) {
        return vertexValue == -1;
    }
}
