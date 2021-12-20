package in.dreamlab.wicm.graph.mutations.resolver;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graph.IntervalVertex;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.util.Map;

public class FASTVertexResolver extends WICMVertexResolver<IntWritable, IntIntIntervalData, IntIntIntervalData> {
    public static final IntConfOption SOURCE_ID = new IntConfOption("sourceId", 1, "EAT Source ID");

    @Override
    protected String vertexToString(Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        StringBuilder sb = new StringBuilder("");
        sb.append(intervalVertex.getId().toString()).append("\t");
        int fastestTime = Integer.MAX_VALUE;
        for(Map.Entry<Range<Integer>, Integer> e : intervalVertex.getValue().getState()){
            if(e.getValue() < Integer.MAX_VALUE)
                fastestTime = Integer.min(fastestTime, (e.getKey().lowerEndpoint()-e.getValue()));
        }
        sb.append(fastestTime);
        return sb.toString();
    }

    @Override
    protected void initialiseState(Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> v) {
        IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?> intervalVertex =
                (IntervalVertex<IntWritable, Integer, Integer, IntIntIntervalData, Integer, IntIntIntervalData, ?, ?, ?>) v;
        intervalVertex.getValue().getPropertyMap().clear();
        if(SOURCE_ID.get(getConf()) == intervalVertex.getId().get()) {
            for(int i = intervalVertex.getLifespan().getStart(); i< intervalVertex.getLifespan().getEnd(); i++){
                intervalVertex.setState(i, i+1, i);
            }
        } else {
            intervalVertex.setState(intervalVertex.getLifespan(), Integer.MAX_VALUE);
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
        intervalVertex.removeState(newIntervalVertex.getLifespan().getEnd(), intervalVertex.getLifespan().getEnd());
        intervalVertex.getLifespan().setEnd(newIntervalVertex.getLifespan().getEnd());
    }
}
