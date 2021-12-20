package in.dreamlab.wicm.io.mutations.formats;

import com.google.common.collect.Lists;
import in.dreamlab.wicm.graphData.UByteIntIntervalData;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import in.dreamlab.wicm.types.VarIntWritable;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class UByteSSSPTextInputFormat extends
        TextVertexInputFormat<VarIntWritable, UByteIntIntervalData, UByteIntIntervalData> {
    private static final IntConfOption LAST_SNAPSHOT = new IntConfOption("lastSnapshot", 255, "Snapshot number at infinity");
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context)
            throws IOException {
        return new IntIntNullVertexReader();
    }

    public class IntIntNullVertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {
        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            return tokens;
        }

        @Override
        protected VarIntWritable getId(String[] tokens) throws IOException {
            return new VarIntWritable(Integer.parseInt(tokens[0]));
        }

        @Override
        protected UByteIntIntervalData getValue(String[] tokens) throws IOException {
            String[] points = tokens[1].split("/");
            int startpoint, endpoint;
            startpoint = Integer.parseInt(points[0]);
            endpoint = (points.length == 2) ? Integer.parseInt(points[1]) : LAST_SNAPSHOT.get(getConf());
            UByteInterval interval = new UByteInterval(startpoint, endpoint);
            return new UByteIntIntervalData(interval);
        }

        @Override
        protected Iterable<Edge<VarIntWritable, UByteIntIntervalData>> getEdges(
                String[] tokens) throws IOException {
            List<Edge<VarIntWritable, UByteIntIntervalData>> edges =
                    Lists.newArrayListWithCapacity(tokens.length - 1);
            for (int n = 2; n < tokens.length; n=n+3) {
                edges.add(EdgeFactory.create(
                        new VarIntWritable(Integer.parseInt(tokens[n])),
                        new UByteIntIntervalData(new UByteInterval(new UnsignedByte(Integer.parseInt(tokens[n+1])), new UnsignedByte(Integer.parseInt(tokens[n+2]))))
                ));
            }
            return edges;
        }
    }
}
