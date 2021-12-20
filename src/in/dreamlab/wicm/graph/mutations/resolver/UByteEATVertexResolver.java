package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class UByteEATVertexResolver extends WICMVertexResolver<IntWritable, UByteIntIntervalData, UByteIntIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, UByteIntIntervalData, UByteIntIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<UnsignedByte>, Integer> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, UByteIntIntervalData, UByteIntIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?>) v;
        intervalVertex.getValue().getPropertyMap().clear();
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), 0);
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Integer.MAX_VALUE);
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, UByteIntIntervalData, UByteIntIntervalData> originalVertex,
                                Vertex<IntWritable, UByteIntIntervalData, UByteIntIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Integer, UByteIntIntervalData, Integer, UByteIntIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
