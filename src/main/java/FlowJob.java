import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;

public class FlowJob {
    static {
        PropertyConfigurator.configure("src/main/resources/log4j.properties");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: word count <in> <out>");
            System.exit(2);
        }

        conf.addResource("src/main/resources/core-site.xml");
        conf.addResource("src/main/resources/hdfs-site.xml");
        conf.addResource("src/main/resources/mapred-site.xml");
        conf.addResource("src/main/resources/yarn-site.xml");

        conf.set("mapreduce.app-submission.cross-platform", "true");

        Job job = Job.getInstance(conf);
        job.setJar("target/phoneflow-1.0.jar");
        job.setJarByClass(FlowJob.class);

        job.setMapperClass(FlowMapper.class);
        job.setCombinerClass(FlowCombiner.class);
        job.setReducerClass(FlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setInputFormatClass(FlowInputFormat.class);
        job.setOutputFormatClass(FlowOutputFormat.class);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(otherArgs[otherArgs.length - 1]), true);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
