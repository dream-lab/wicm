package in.dreamlab.wicm.io.formats;

import com.google.common.collect.Lists;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.graphData.IntPairIntIntervalData;
import in.dreamlab.graphite.types.IntInterval;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

public class IntPairIntIntNullTextInputFormat extends
        TextVertexInputFormat<IntWritable, IntPairIntIntervalData, IntIntIntervalData> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
                                               TaskAttemptContext context)
            throws IOException {
        return new IntPairIntIntNullVertexReader();
    }

    /**
     * Vertex reader associated with {@link IntPairIntIntNullTextInputFormat}.
     */
    public class IntPairIntIntNullVertexReader extends
            TextVertexReaderFromEachLineProcessed<String[]> {
        /**
         * Cached vertex id for the current line
         */
        private IntWritable id;

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            String[] tokens = SEPARATOR.split(line.toString());
            id = new IntWritable(Integer.parseInt(tokens[0]));
            return tokens;
        }

        @Override
        protected IntWritable getId(String[] tokens) throws IOException {
            return id;
        }

        @Override
        protected IntPairIntIntervalData getValue(String[] tokens) throws IOException {
            return new IntPairIntIntervalData(new IntInterval(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2])));
        }

        @Override
        protected Iterable<Edge<IntWritable, IntIntIntervalData>> getEdges(
                String[] tokens) throws IOException {
            List<Edge<IntWritable, IntIntIntervalData>> edges =
                    Lists.newArrayListWithCapacity(tokens.length - 1);
            for (int n = 3; n < tokens.length; n=n+3) {
                edges.add(EdgeFactory.create(
                        new IntWritable(Integer.parseInt(tokens[n])),
                        new IntIntIntervalData(new IntInterval(Integer.parseInt(tokens[n+1]), Integer.parseInt(tokens[n+2])))
                ));
            }
            return edges;
        }
    }
}
