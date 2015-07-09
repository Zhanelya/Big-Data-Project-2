import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	Map<Text, Integer> nodes = new HashMap<Text, Integer>();
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		   StringBuilder concat = new StringBuilder();
           for (Text value : values) {
    		  concat.append(value); 
    		  concat.append(",");
           }
           concat.setLength(concat.length() - 1);
           result.set(concat.toString());
           context.write(key, result); 
         }
	                                          
}