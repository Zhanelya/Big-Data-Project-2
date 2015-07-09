import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelPropagationReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	private Text result = new Text();
    private long lminLabel=Long.MAX_VALUE; 
	private String sNodeList="" ;
	private long lLabel=Long.MAX_VALUE;
	private String sLabel="";
	private String input; 
	private long origLabel = 0;
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{ 
	   for (Text value : values) {  
	   input = value.toString();                
           if(input.split(" ").length > 0)
		  {       	   
        
        	sLabel=input.split(" ")[0];
                   lLabel=Long.parseLong(sLabel);
      	
         	//if exists the adjacent List
	        if(Math.abs(lLabel) < Math.abs(lminLabel)){
	        		lminLabel = lLabel;
	        	}
	            
	        if(input.split(" ").length == 2){
	        		origLabel = Long.parseLong(input.split(" ")[1]);
                	}
	        if(input.split(" ").length == 3){
	        	sNodeList=input.split(" ")[1];
	   		  	origLabel = Long.parseLong(input.split(" ")[2]);
	   	  	}
		  }       	
	  }   
	  
	  
	  if(origLabel!=lminLabel){
		  context.getCounter(LabelPropagation.UpdateCounter.UPDATED)
			.increment(1);
	  }
	   
	  if(lminLabel>0){
		result.set("+"+lminLabel+" "+ sNodeList);
		//+" "+origLabel+" "+lminLabel+" "+context.getCounter(LabelPropagation.UpdateCounter.UPDATED).getValue()
	  }else{
		//if it's -, then it's already having it in front unlike the plus
        result.set(lminLabel +" "+ sNodeList);
      }   
	  
	  context.write(key, result);
	  //result.set(values.toString());
	  //context.write(key, result);
	  
    }
}
