package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntBooleanIntervalData;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class REACHDVertexResolver extends WICMVertexResolver<IntWritable, IntBooleanIntervalData, IntIntIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, IntBooleanIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<Integer>, Boolean> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, IntBooleanIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), true);
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), false);
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, IntBooleanIntervalData, IntIntIntervalData> originalVertex,
                                Vertex<IntWritable, IntBooleanIntervalData, IntIntIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, Integer, Boolean, IntBooleanIntervalData, Integer, IntIntIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
