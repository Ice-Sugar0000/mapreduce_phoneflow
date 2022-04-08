import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws IOException, InterruptedException {
        BigInteger upFlow = new BigInteger("0");
        BigInteger downFlow = new BigInteger("0");
        BigInteger allFlow = new BigInteger("0");

        for (FlowBean value : values) {
            upFlow = upFlow.add(new BigInteger(value.getUpFlow()));
            downFlow = downFlow.add(new BigInteger(value.getDownFlow()));
            allFlow = allFlow.add(new BigInteger(value.getAllFlow()));
        }
        context.write(key, new FlowBean(upFlow.toString(), downFlow.toString(), allFlow.toString()));
    }
}
