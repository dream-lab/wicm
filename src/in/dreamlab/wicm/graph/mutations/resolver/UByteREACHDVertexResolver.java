package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.wicm.graphData.UByteBooleanIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class UByteREACHDVertexResolver extends WICMVertexResolver<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<UnsignedByte>, Boolean> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?>) v;
        intervalVertex.getValue().getPropertyMap().clear();
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), true);
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), false);
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> originalVertex,
                                Vertex<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Boolean, UByteBooleanIntervalData, Boolean, UByteBooleanIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
