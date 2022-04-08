import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowOutputFormat extends FileOutputFormat<Text, FlowBean> {
    @Override
    public RecordWriter<Text, FlowBean> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String url = conf.get("mapreduce.output.fileoutputformat.outputdir");
        Path path = new Path(url == null ? null : url + "/out.txt");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream output = fs.create(path);
        return new FlowRecordWriter(output);
    }
}
