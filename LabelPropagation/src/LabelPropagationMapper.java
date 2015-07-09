import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LabelPropagationMapper extends Mapper<LongWritable, Text, LongWritable, Text> { 
   Text result =new Text();
   private LongWritable user = new LongWritable();
   private Text data = new Text();
   private Text label_text = new Text();
   String[] input;

   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
   {
	  //Getting the input line
	   input = value.toString().split("\t");
	   if(input.length>1)
	   {
		   user.set(Long.parseLong(input[0]));
		   
		   if(input[1].split(" ").length>0){
			   label_text.set(input[1].split(" ")[0]);
		   }else{
			   label_text.set(input[1]);
		   }
		   
		   StringBuilder concat = new StringBuilder();
		   concat.append(input[1]);
           concat.append(" ");
           concat.append(label_text);
		   data.set(concat.toString());
	
		   context.write(user, data);
		   System.out.println("parent"+" "+user+" "+concat);
		   
		   if(input[1].split(" ").length > 1){
			   StringTokenizer st = new StringTokenizer((input[1].split(" ")[1]), ",");
			   while (st.hasMoreElements()) {
			   user.set(Long.parseLong(st.nextToken()));
			   context.write(user, label_text);
			   System.out.println("child"+" "+user+" "+label_text);
				  
			   }
		   }
		   
	   }
       
   	}
   
}
   
          
