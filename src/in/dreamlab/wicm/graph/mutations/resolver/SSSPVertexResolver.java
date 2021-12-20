package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntDoubleIntervalData;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class SSSPVertexResolver extends WICMVertexResolver<IntWritable, IntDoubleIntervalData, IntDoubleIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, IntDoubleIntervalData, IntDoubleIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        for(Map.Entry<Range<Integer>, Double> stateEntry : intervalVertex.getState()) {
            sb.append("[").append(stateEntry.getKey().lowerEndpoint().toString());
            sb.append(",").append(stateEntry.getKey().upperEndpoint().toString()).append(")");
            sb.append("\t");
            sb.append(stateEntry.getValue().toString());
            sb.append("\t");
        }
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, IntDoubleIntervalData, IntDoubleIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?>) v;
        if(intervalVertex.getId().get() == SOURCE_ID.get(getConf())) {
            intervalVertex.setState(intervalVertex.getLifespan(), 0.);
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Double.POSITIVE_INFINITY);
            intervalVertex.voteToHalt();
        }
    }

    @Override
    protected void customAction(Vertex<IntWritable, IntDoubleIntervalData, IntDoubleIntervalData> originalVertex,
                                Vertex<IntWritable, IntDoubleIntervalData, IntDoubleIntervalData> newVertex) {
        // truncate the endpoint of the vertex
        IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?>)originalVertex;
        IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?> newIntervalVertex =
                (IntervalVertex<IntWritable, Integer, Double, IntDoubleIntervalData, Double, IntDoubleIntervalData, ?, ?, ?>)newVertex;
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
