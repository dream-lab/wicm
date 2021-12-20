package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class LDVertexResolver extends WICMVertexResolver<IntWritable, IntIntIntervalData, IntIntIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<Integer>, Integer> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), intervalVertex.getLifespan().getEnd() - 1);
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), -1);
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> originalVertex,
                                Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(intervalVertex.getLifespan().getStart(), newIntervalVertex.getLifespan().getStart());
        intervalVertex.getLifespan().setStart(newIntervalVertex.getLifespan().getStart());
    }
}
