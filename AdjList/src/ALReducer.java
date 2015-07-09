import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ALReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
	Text result = new Text();
       public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
           StringBuilder concat = new StringBuilder();
           concat.append("+");
           concat.append(key);
           concat.append(" ");
           for (LongWritable value : values) {
             concat.append(value.get());
             concat.append(",");
           }
           concat.setLength(concat.length() - 1);
           result.set(concat.toString());
           context.write(key, result); 
         }

}