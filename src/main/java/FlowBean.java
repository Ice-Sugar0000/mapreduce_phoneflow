import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
    private String upFlow;
    private String downFlow;
    private String allFlow;

    public FlowBean() {
        this.upFlow = "0";
        this.downFlow = "0";
        this.allFlow = "0";
    }

    public FlowBean(String upFlow, String downFlow, String allFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.allFlow = allFlow;
    }

    public String getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(String upFlow) {
        this.upFlow = upFlow;
    }

    public String getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(String downFlow) {
        this.downFlow = downFlow;
    }

    public String getAllFlow() {
        return allFlow;
    }

    public void setAllFlow(String allFlow) {
        this.allFlow = allFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(upFlow);
        dataOutput.writeUTF(downFlow);
        dataOutput.writeUTF(allFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readUTF();
        downFlow = dataInput.readUTF();
        allFlow = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "{" +
                "upFlow='" + upFlow + '\'' +
                ", downFlow='" + downFlow + '\'' +
                ", allFlow='" + allFlow + '\'' +
                '}';
    }
}
