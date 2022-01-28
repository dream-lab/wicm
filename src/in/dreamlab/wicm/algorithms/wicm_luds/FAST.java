package in.dreamlab.wicm.algorithms.wicm_luds;

import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.comm.messages.IntStartSlimMessage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class FAST extends
        DebugIntWindowIntervalComputation<IntWritable, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntStartSlimMessage> {

    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "FAST Source ID");
    public static final Integer travelDuration = 1;

    @Override
    public boolean init(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntStartSlimMessage> intervalVertex) {
        intervalVertex.getValue().getPropertyMap().clear();
        if (intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            for(int i=intervalVertex.getLifespan().getStart(); i<intervalVertex.getLifespan().getEnd(); i++){
                intervalVertex.setState(i, i+1, i);
            }
            return true;
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Integer.MAX_VALUE);
        }
        return false;
    }

    @Override
    public Collection<Pair<Interval<Integer>, Integer>> compute(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntStartSlimMessage> intervalVertex,
            Interval<Integer> interval, Integer currentDepartureTime,
            Integer candidateDepartureTime) throws IOException {
        if(currentDepartureTime == Integer.MAX_VALUE || currentDepartureTime.compareTo(candidateDepartureTime) < 0){
            intervalVertex.setState(interval, candidateDepartureTime);
            return Collections.singleton(new ImmutablePair<>(interval, candidateDepartureTime));
        }
        return Collections.emptySet();
    }

    @Override
    public Iterable<IntStartSlimMessage> scatter(
            IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, Integer, Integer, IntStartSlimMessage> intervalVertex,
            Edge<IntWritable, IntIntIntervalData> edge, Interval<Integer> interval,
            Integer currentDepartureTime, Integer nullProperty) {
        int arrivalTime = interval.getStart() + travelDuration;
        return Collections.singleton(new IntStartSlimMessage(arrivalTime, currentDepartureTime));
    }

    @Override
    protected Character getPropertyLabelForScatter() {
        return null;
    }

    @Override
    public boolean isDefault(Integer vertexValue) {
        return vertexValue == Integer.MAX_VALUE;
    }
}
