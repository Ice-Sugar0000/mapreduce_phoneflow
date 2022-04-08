import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class FlowRecordWriter extends RecordWriter<Text, FlowBean> {
    FSDataOutputStream outputStream;

    public FlowRecordWriter(FSDataOutputStream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public void write(Text text, FlowBean flowBean) throws IOException, InterruptedException {
        outputStream.write((text.toString() + flowBean.toString() + "\n").getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (outputStream != null) {
            outputStream.close();
        }
    }
}
