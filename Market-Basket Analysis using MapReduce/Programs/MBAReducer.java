import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by qiang on 1/10/15.
 */
public class MBAReducer extends Reducer<ProductPair, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(ProductPair pair, Iterable<IntWritable> vs, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : vs) {
            count++;
        }

        context.write(new Text("(" + pair.getProduct1() + ", " + pair.getProduct2() + ")"), new IntWritable(count));
    }
}
