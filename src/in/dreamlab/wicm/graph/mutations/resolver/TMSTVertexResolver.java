package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.graphData.IntPairIntIntervalData;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class TMSTVertexResolver extends WICMVertexResolver<IntWritable, IntPairIntIntervalData, IntIntIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, IntPairIntIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<Integer>, Pair<Integer,Integer>> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, IntPairIntIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(intervalVertex.getId().get(), 0));
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(-1, Integer.MAX_VALUE));
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, IntPairIntIntervalData, IntIntIntervalData> originalVertex,
                                Vertex<IntWritable, IntPairIntIntervalData, IntIntIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, Integer, Pair<Integer,Integer>, IntPairIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
