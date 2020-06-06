import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by qiang on 1/10/15.
 */
public class MBAMapper extends Mapper<LongWritable, Text, ProductPair, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<String> tokens = Arrays.asList(value.toString().trim().split(","));
        tokens.sort((v1, v2) -> v1.compareTo(v2));
        for (int i = 0; i< tokens.size(); i++) {
            for (int j = i + 1; j < tokens.size(); j++) {
                ProductPair pair = new ProductPair();
                pair.setProduct1(tokens.get(i));
                pair.setProduct2(tokens.get(j));
                context.write(pair, new IntWritable(1));
            }
        }
    }
}
