import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CPNMapper extends Mapper<Object, Text, LongWritable, Text> { 
   private LongWritable user = new LongWritable();
   private Text data = new Text();
   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   	String[] users = value.toString().split("\t");
   	if(users.length>1){
   		user.set(Math.abs(Long.parseLong(users[0])));
   	    StringTokenizer st = new StringTokenizer(users[1], ",");
   		while (st.hasMoreElements()) {
   			data.set(st.nextToken());
   			context.write(user, data);
   		}
   	}
   }
        
}    

