import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class FlowInputFormat extends FileInputFormat<Text, Text> {
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        FlowRecordReader flowRecordReader = new FlowRecordReader();
        flowRecordReader.initialize(split, context);
        return flowRecordReader;
    }
}
