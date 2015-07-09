import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MMapper extends Mapper<Object, Text, Text, Text> { 
   private Text label = new Text();
   private Text user = new Text();
   
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   	String[] users = value.toString().split("\t");
   	if(users.length>1){
   		label.set(users[1].split(" ")[0]);
   	    user.set(users[0]);	
		context.write(label, user);
   	}
   }
        
}    

