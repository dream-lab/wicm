package in.dreamlab.wicm.conf;

import in.dreamlab.wicm.graph.mutations.WICMMutationsWorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;

public class ResolverDumpOutput extends TextOutputFormat<Text,Text> {
    private static final Logger LOG = Logger.getLogger(ResolverDumpOutput.class);
    String separator = "\t";

    DataOutputStream ioStream;
    RecordWriter<Text,Text> writer;

    public ResolverDumpOutput(){
    }

    public void initialise(String path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path filenamePath = new Path(path);
        FSDataOutputStream fin = fs.create(filenamePath, true);
        ioStream = fin;
        writer = new LineRecordWriter<>(fin, separator);
    }

    public void close() throws IOException {
        if(ioStream != null) {
            LOG.info("Closing...");
            ioStream.close();
        }
        writer = null;
        ioStream = null;
    }

    public void write(Text string) throws IOException, InterruptedException {
        writer.write(string, null);
    }
}
