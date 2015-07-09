import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

 public class Merger {

  public static void runJob(String[] input, String output) throws Exception {

	Job job = Job.getInstance(new Configuration());
    Configuration conf = job.getConfiguration();
     
	//conf.set("mapreduce.reduce.shuffle.input.buffer.percent", ".20");
	
	
	
    job.setJarByClass(Merger.class);
    job.setMapperClass(MMapper.class);
    job.setReducerClass(MReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.setNumReduceTasks(20);
    job.waitForCompletion(true);
     
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}
