package in.dreamlab.wicm.algorithms.icm_luds;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.UByteIntStartSlimMessage;
import in.dreamlab.wicm.graph.computation.DebugBlockWarpBasicIntervalComputation;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import in.dreamlab.wicm.types.VarIntWritable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class UByteSSSP extends
        DebugBlockWarpBasicIntervalComputation<VarIntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");
    public static final int travelTime = 1;
    public static final int travelDistance = 1;

    @Override
    public boolean init(
            IntervalVertex<VarIntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> intervalVertex) {
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
            IntervalVertex<VarIntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> intervalVertex,
            Interval<UnsignedByte> interval, Integer currentDistance, Integer candidateDistance) throws IOException {
        if (currentDistance > candidateDistance) {
            intervalVertex.setState(interval, candidateDistance);
            return Collections.singleton(new ImmutablePair<>(interval, candidateDistance));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<UByteIntStartSlimMessage> scatter(
            IntervalVertex<VarIntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, Integer, Integer, UByteIntStartSlimMessage> intervalVertex,
            Edge<VarIntWritable, UByteIntIntervalData> edge, Interval<UnsignedByte> interval, Integer vState,
            Integer nullProperty) {
        return Collections.singleton(new UByteIntStartSlimMessage(new UnsignedByte(interval.getStart().intValue() + travelTime), vState + travelDistance));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

}

