
 
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelPropagation {
	public enum UpdateCounter {
		  UPDATED
		 };
		 
    public static void runJob(String[] input, String output) throws Exception {
    	  //hadoojar dist/LabelPropagation.jar LabelPropagation output_of_adjlist_algorithm files/depth_0
  	      //e.g. hadoop jar dist/LabelPropagation.jar LabelPropagation out/*/* files/depth_0
    	
    	  Configuration conf = new Configuration();
		  Job job = new Job(conf);
		  FileSystem fs = FileSystem.get(conf);
		  job.setJobName("Label Propagation");
		  job.setJarByClass(LabelPropagation.class);
		  job.setMapperClass(LabelPropagationMapper.class);
		  job.setReducerClass(LabelPropagationReducer.class);
		  job.setMapOutputKeyClass(LongWritable.class);
		  job.setMapOutputValueClass(Text.class);
		  job.setNumReduceTasks(20);
		    
		  conf.set("mapreduce.reduce.shuffle.input.buffer.percent", ".20");
		  Path outputPath_initial = new Path(output);
		    
		  FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		  FileOutputFormat.setOutputPath(job, outputPath_initial);
		    
		  outputPath_initial.getFileSystem(conf).delete(outputPath_initial,true);
		  job.waitForCompletion(true);
		  // variable to keep track of the recursion depth
		  int depth = 0;
		  // counter from the previous running import job
		  long counter = job.getCounters().findCounter(UpdateCounter.UPDATED)
		     .getValue();
    			  
    			   depth++;
    			   while (counter > 0) {
    				job.getCounters().findCounter(UpdateCounter.UPDATED).setValue(0);
    			    // reuse the conf reference with a fresh object
    			    conf = new Configuration();
    			    // set the depth into the configuration
    			    conf.set("recursion.depth", depth + "");
    			    job = new Job(conf);
    			    job.setJobName("Graph explorer " + depth);
    			    
    			    job.setNumReduceTasks(20);
    				  
    			    job.setMapperClass(LabelPropagationMapper.class);
    			    job.setReducerClass(LabelPropagationReducer.class);
    			    job.setJarByClass(LabelPropagation.class);
    			    // always work on the path of the previous depth
    			    Path inputPath = new Path("files/depth_" + (depth - 1) + "/");
    			    Path outputPath = new Path("files/depth_" + depth);
    			    FileInputFormat.setInputPaths(job, inputPath);
    				FileOutputFormat.setOutputPath(job, outputPath);
    				if (fs.exists(outputPath))
    				    fs.delete(outputPath, true);
    				
    			    job.setOutputKeyClass(LongWritable.class);
    			    job.setOutputValueClass(Text.class);
    			    // wait for completion and update the counter
    			    job.waitForCompletion(true);
    			    depth++;
    			    counter = job.getCounters().findCounter(UpdateCounter.UPDATED)
    			      .getValue();
    			   }
   
  }

  public static void main(String[] args) throws Exception {
       
	  runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
	  //runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }



}
