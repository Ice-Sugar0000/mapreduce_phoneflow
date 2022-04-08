import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<Text, Text, Text, FlowBean> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        String flow = strings[0];
        String type = strings[1];
        if (type.matches("up")) {
            context.write(key, new FlowBean(flow, "0", flow));
        } else {
            context.write(key, new FlowBean("0", flow, flow));
        }
    }
}
