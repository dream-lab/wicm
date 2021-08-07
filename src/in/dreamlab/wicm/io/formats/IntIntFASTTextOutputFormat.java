package in.dreamlab.wicm.io.formats;

import com.google.common.collect.Range;
import in.dreamlab.graphite.graphData.IntIntIntervalData;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map.Entry;

public class IntIntFASTTextOutputFormat
        extends TextVertexOutputFormat<IntWritable, IntIntIntervalData, IntIntIntervalData> {

    /** Specify the output delimiter */
    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    /** Default output delimiter */
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
    /** Reverse id and value order? */
    public static final String REVERSE_ID_AND_VALUE = "reverse.id.and.value";
    /** Default is to not reverse id and value order. */
    public static final boolean REVERSE_ID_AND_VALUE_DEFAULT = false;

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new IdWithValueVertexWriter();
    }

    /**
     * Vertex writer used with {@link IntIntFASTTextOutputFormat}.
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
        protected Text convertVertexToLine(Vertex<IntWritable, IntIntIntervalData, IntIntIntervalData> vertex)
                throws IOException {
            StringBuilder str = new StringBuilder();
            str.append(vertex.getId().toString());
            str.append(delimiter);

            int fastestTime = Integer.MAX_VALUE;
            for(Entry<Range<Integer>, Integer> e : vertex.getValue().getState()){
                if(e.getValue() < Integer.MAX_VALUE)
                    fastestTime = Integer.min(fastestTime, (e.getKey().lowerEndpoint()-e.getValue()));
            }
            str.append(fastestTime);

            return new Text(str.toString());
        }
    }
}
