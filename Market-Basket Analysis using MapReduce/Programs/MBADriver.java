import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by qiang on 1/10/15.
 */
public class MBADriver extends Configured implements Tool {
    @Override
    public int run(String... args) throws Exception {
        Job job = new Job(getConf());

        job.setJarByClass(MBAMapper.class);
        job.setJarByClass(MBAReducer.class);
        job.setJarByClass(MBADriver.class);

        job.setMapperClass(MBAMapper.class);
        job.setReducerClass(MBAReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(ProductPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String...args) throws Exception {
        ToolRunner.run(new MBADriver(), args);
    }
}
