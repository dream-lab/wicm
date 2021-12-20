package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.wicm.graphData.UBytePairUByteIntIntervalData;
import in.dreamlab.wicm.graphData.UByteUByteIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class UByteTMSTVertexResolver extends WICMVertexResolver<IntWritable, UBytePairUByteIntIntervalData, UByteUByteIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, UBytePairUByteIntIntervalData, UByteUByteIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<UnsignedByte>, Pair<UnsignedByte,Integer>> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, UBytePairUByteIntIntervalData, UByteUByteIntervalData> v) {
        IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>) v;
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(new UnsignedByte(0), intervalVertex.getId().get()));
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), new MutablePair<>(new UnsignedByte(UnsignedByte.MAX_VALUE), -1));
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, UBytePairUByteIntIntervalData, UByteUByteIntervalData> originalVertex,
                                Vertex<IntWritable, UBytePairUByteIntIntervalData, UByteUByteIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, UnsignedByte, Pair<UnsignedByte,Integer>, UBytePairUByteIntIntervalData, UnsignedByte, UByteUByteIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
