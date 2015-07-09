import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ALMapper extends Mapper<Object, Text, LongWritable, LongWritable> { 
   private LongWritable user = new LongWritable();
   private LongWritable follower = new LongWritable();
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   	String[] users = value.toString().split("\t");
   	if(users.length>1){
   		user.set(Long.parseLong(users[0]));
   	   	follower.set(Long.parseLong(users[1]));
   	   	context.write(user, follower);
   	}
   }
        
}    

