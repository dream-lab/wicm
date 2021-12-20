package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.wicm.graphData.UByteUByteIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class UByteFASTVertexResolver extends WICMVertexResolver<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        int fastestTime = Integer.MAX_VALUE;
        for(Map.Entry<Range<UnsignedByte>, UnsignedByte> e : intervalVertex.getValue().getState()){
            if(e.getValue().intValue() != UnsignedByte.MAX_VALUE)
                fastestTime = Integer.min(fastestTime, (e.getKey().lowerEndpoint().intValue() - e.getValue().intValue()));
        }
        sb.append(fastestTime);
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>) v;
        intervalVertex.getValue().getPropertyMap().clear();
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            for(int i=intervalVertex.getLifespan().getStart().intValue(); i<intervalVertex.getLifespan().getEnd().intValue(); i++){
                intervalVertex.setState(new UnsignedByte(i), new UnsignedByte(i+1), new UnsignedByte(i));
            }
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), new UnsignedByte(UnsignedByte.MAX_VALUE));
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> originalVertex,
                                Vertex<IntWritable, UByteUByteIntervalData, UByteUByteIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, UnsignedByte, UByteUByteIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
