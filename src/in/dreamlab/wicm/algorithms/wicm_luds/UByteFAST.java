package in.dreamlab.wicm.algorithms.wicm_luds;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.UByteStartSlimMessage;
import in.dreamlab.wicm.graphData.UByteUByteIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class UByteFAST extends
        DebugUByteWindowIntervalComputation<IntWritable, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UnsignedByte, UByteStartSlimMessage> {

    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "FAST Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UnsignedByte, UByteStartSlimMessage> intervalVertex) {
        intervalVertex.getValue().getPropertyMap().clear();
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            for(int i=intervalVertex.getLifespan().getStart().intValue(); i<intervalVertex.getLifespan().getEnd().intValue(); i++){
                intervalVertex.setState(new UnsignedByte(i), new UnsignedByte(i+1), new UnsignedByte(i));
            }
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), new UnsignedByte(UnsignedByte.MAX_VALUE));
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<UnsignedByte>, UnsignedByte>> compute(
            IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UnsignedByte, UByteStartSlimMessage> intervalVertex,
            Interval<UnsignedByte> interval, UnsignedByte currentDepartureTime,
            UnsignedByte candidateDepartureTime) throws IOException {
        if(currentDepartureTime.intValue() == UnsignedByte.MAX_VALUE || currentDepartureTime.compareTo(candidateDepartureTime) < 0){
            intervalVertex.setState(interval, candidateDepartureTime);
            return Collections.singleton(new ImmutablePair<>(interval, candidateDepartureTime));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<UByteStartSlimMessage> scatter(
            IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UnsignedByte, UByteStartSlimMessage> intervalVertex,
            Edge<IntWritable, UByteUByteIntervalData> edge, Interval<UnsignedByte> interval,
            UnsignedByte currentDepartureTime, UnsignedByte nullProperty) {
        int arrivalTime = interval.getStart().intValue() + travelDuration;
        return Collections.singleton(new UByteStartSlimMessage(new UnsignedByte(arrivalTime), currentDepartureTime));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

    @Override
    public boolean isDefault(UnsignedByte vertexValue) {
        return vertexValue.intValue() == UnsignedByte.MAX_VALUE;
    }
}