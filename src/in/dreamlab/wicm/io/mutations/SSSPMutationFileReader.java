package in.dreamlab.wicm.io.mutations;

import com.google.common.collect.Lists;
import in.dreamlab.graphite.graphData.IntDoubleIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.List;

public class SSSPMutationFileReader extends WICMMutationFileReader<IntWritable, IntDoubleIntervalData, IntDoubleIntervalData> {
    private static final IntConfOption LAST_SNAPSHOT = new IntConfOption("lastSnapshot", 1000, "Snapshot number at infinity");

    @Override
    IntWritable setVertexId(String[] line) {
        return new IntWritable(Integer.parseInt(line[1]));
    }

    @Override
    IntDoubleIntervalData setVertexValue(String[] line) {
        if(getMode() == MODE.DELETE_VERTEX || getMode() == MODE.REPLACE_EDGE)
            return null;

        String[] points = line[2].split("/");
        int startpoint,endpoint;
        if(getMode() == MODE.ADD_VERTEX) {
            startpoint = Integer.parseInt(points[0]);
            endpoint = (points.length == 2) ? Integer.parseInt(points[1]) : LAST_SNAPSHOT.get(getConf());
        } else {
            startpoint = Integer.MIN_VALUE;
            endpoint = Integer.parseInt(points[0]);
        }

        return new IntDoubleIntervalData(new IntInterval(startpoint, endpoint));
    }

    @Override
    List<Edge<IntWritable, IntDoubleIntervalData>> setEdges(String[] line) {
        if(getMode() == MODE.DELETE_VERTEX || getMode() == MODE.TRUNCATE_VERTEX)
            return null;

        int startIndex = (getMode() == MODE.ADD_VERTEX) ? 3 : 2;
        List<Edge<IntWritable, IntDoubleIntervalData>> edges =
                Lists.newArrayListWithCapacity((line.length - startIndex)/3);
        for (int n = startIndex; n < line.length; n=n+3) {
            edges.add(EdgeFactory.create(
                    new IntWritable(Integer.parseInt(line[n])),
                    new IntDoubleIntervalData(new IntInterval(Integer.parseInt(line[n+1]), Integer.parseInt(line[n+2])))
            ));
        }
        return edges;
    }
}
