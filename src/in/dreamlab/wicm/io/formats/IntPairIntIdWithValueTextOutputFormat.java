package in.dreamlab.wicm.io.formats;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import in.dreamlab.graphite.graphData.IntPairIntIntervalData;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map.Entry;

public class IntPairIntIdWithValueTextOutputFormat
        extends TextVertexOutputFormat<IntWritable, IntPairIntIntervalData, IntIntIntervalData> {

    /** Specify the output delimiter */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default output delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new IdWithValueVertexWriter();
    }

    /**
     * Vertex writer used with {@link IntPairIntIdWithValueTextOutputFormat}.
     */
    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
        /** Saved delimiter */
        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<IntWritable, IntPairIntIntervalData, IntIntIntervalData> vertex)
                throws IOException {
            StringBuilder str = new StringBuilder();
            str.append(vertex.getId().toString());
            str.append(delimiter);

            for (Entry<Range<Integer>, Pair<Integer,Integer>> stateEntry : vertex.getValue().getState()) {
                str.append("[" + stateEntry.getKey().lowerEndpoint().toString());
                str.append("," + stateEntry.getKey().upperEndpoint().toString() + ")");
                str.append(delimiter);
                str.append(stateEntry.getValue().toString());
                str.append(delimiter);
            }
            return new Text(str.toString());
        }
    }
}
