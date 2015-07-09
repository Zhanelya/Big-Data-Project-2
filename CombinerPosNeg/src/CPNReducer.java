import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CPNReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
	Text result = new Text();
	String[] strarr;
	Map<Text, Integer> nodes = new HashMap<Text, Integer>();
	    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		   StringBuilder concat = new StringBuilder();
           for (Text value : values) {
        	   if(nodes.containsKey(value)){
        		  concat.append(value.toString().trim());
        		  concat.append(",");
        	   }else{
        		  nodes.put(value, 1);
        	   }
           }
           if(concat.length()>0) {
        	   concat.setLength(concat.length() - 1);
        	   strarr = concat.toString().split(",");
        	   for(String str:strarr){
        		   result.set(str);
                   context.write(key, result); 
        	   }
        	   
           }
           nodes.clear();
         }

	    
	    /*
	    public static String removeDuplicates (StringBuilder aString)
	     {
	      String[] stringList =  aString.toString().split(",");
	      Set<String> unique = new HashSet<String>(Arrays.asList(stringList));
	      String result = unique.toString();
	      result = result.substring(1,result.length()-1);
	      return result;
	     }
		*/
}