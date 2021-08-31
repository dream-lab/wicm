package in.dreamlab.wicm.io.formats;

import com.google.common.collect.Range;
import in.dreamlab.wicm.graphData.UByteBooleanIntervalData;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Map.Entry;

public class UByteBooleanIdWithValueTextOutputFormat
        extends TextVertexOutputFormat<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> {

    public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";
    public static final String REVERSE_ID_AND_VALUE = "reverse.id.and.value";
    public static final boolean REVERSE_ID_AND_VALUE_DEFAULT = false;

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new IdWithValueVertexWriter();
    }

    protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
        private String delimiter;

        @Override
        public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
            super.initialize(context);
            delimiter = getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
        }

        @Override
        protected Text convertVertexToLine(Vertex<IntWritable, UByteBooleanIntervalData, UByteBooleanIntervalData> vertex)
                throws IOException {
            StringBuilder str = new StringBuilder();
            str.append(vertex.getId().toString());
            str.append(delimiter);

            for (Entry<Range<UnsignedByte>, Boolean> stateEntry : vertex.getValue().getState()) {
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
