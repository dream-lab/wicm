package in.dreamlab.wicm.algorithms.block_icm;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.UBytePairUByteIntStartSlimMessage;
import in.dreamlab.wicm.graph.computation.DebugBlockWarpBasicIntervalComputation;
import in.dreamlab.wicm.graphData.UBytePairUByteIntIntervalData;
import in.dreamlab.wicm.graphData.UByteUByteIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class UByteTMST extends
        DebugBlockWarpBasicIntervalComputation<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, Pair<UnsignedByte, Integer>, Pair<UnsignedByte, Integer>, UBytePairUByteIntStartSlimMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, Pair<UnsignedByte, Integer>, Pair<UnsignedByte, Integer>, UBytePairUByteIntStartSlimMessage> intervalVertex) {
        intervalVertex.getValue().getPropertyMap().clear();
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(new UnsignedByte(0), intervalVertex.getId().get()));
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(new UnsignedByte(UnsignedByte.MAX_VALUE), -1));
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<UnsignedByte>, Pair<UnsignedByte,Integer>>> compute(
            IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, Pair<UnsignedByte, Integer>, Pair<UnsignedByte, Integer>, UBytePairUByteIntStartSlimMessage> intervalVertex,
            Interval<UnsignedByte> interval, Pair<UnsignedByte, Integer> currentParent, Pair<UnsignedByte, Integer> candidateParent) throws IOException {
        if (candidateParent.getLeft().compareTo(currentParent.getLeft()) < 0) {
            intervalVertex.setState(interval, candidateParent);
            return Collections.singleton(new ImmutablePair<>(interval, candidateParent));
        } else if(candidateParent.getLeft().equals(currentParent.getLeft())){
            if(candidateParent.getRight() < currentParent.getRight())
                intervalVertex.setState(interval,candidateParent);
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<UBytePairUByteIntStartSlimMessage> scatter(
            IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, Pair<UnsignedByte, Integer>, Pair<UnsignedByte, Integer>, UBytePairUByteIntStartSlimMessage> intervalVertex,
            Edge<IntWritable, UByteUByteIntervalData> edge, Interval<UnsignedByte> interval, Pair<UnsignedByte,Integer> currentParent, UnsignedByte nullProperty) {
        int reachingTime = Integer.max(currentParent.getLeft().intValue(), interval.getStart().intValue()) + travelDuration;
        UnsignedByte reachingTimeByte = new UnsignedByte(reachingTime);
        return Collections.singleton(new UBytePairUByteIntStartSlimMessage(reachingTimeByte, new MutablePair<>(reachingTimeByte, intervalVertex.getId().get())));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }
}
