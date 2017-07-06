package MovieLens;
import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.tools.javac.comp.Todo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRdriver extends Configured implements Tool {

   public int run(String[] args) throws Exception {
	   
	    
      // TODO: configure first MR job 
	  Job job1 = new Job(getConf(), "Filter Movies and Ranking Data"); 
      job1.setJarByClass(MRdriver.class);
	  // TODO: setup input and output paths for first MR job
	  MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Movies.class);
      MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Ratings.class);
      FileOutputFormat.setOutputPath(job1, new Path(args[2]));
	  
	  job1.setReducerClass(MRreducer1.class);
	  job1.setOutputKeyClass(Text.class);
	  job1.setOutputValueClass(Text.class);
	  job1.setMapOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job1, new Path(args[0]));
	  
      // TODO: run first MR job syncronously with verbose output set to true
	  if(!job1.waitForCompletion(true)){
		  return 1;
	  }
	  // TODO: configure the second MR job 
	  
	  job1 = new Job(getConf(), "Solution to Problem Statement 1");
	  job1.setJarByClass(MRdriver.class);
	  
      // TODO: setup input and output paths for Second MR job for problem Statement 1
	  
	  job1.setMapperClass(MRmapper2.class);
	  job1.setReducerClass(MRreducerSolution1.class);
	  job1.setInputFormatClass(TextInputFormat.class);
	  job1.setOutputKeyClass(Text.class);
	  job1.setOutputValueClass(Text.class);
	  job1.setMapOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job1, new Path(args[2]));
	  FileOutputFormat.setOutputPath(job1, new Path(args[3]));
      
	  // setup input and output paths for Second MR job for problem Statement 2 
	  Job job2 = new Job(getConf(), "Solution to Problem Statement 2");
	  job2.setJarByClass(MRdriver.class);
	  
	  // TODO: setup input and output paths for second MR job for problem Statement 2
	  
	  job2.setMapperClass(MRmapper2.class);
	  job2.setReducerClass(MRreducerSolution2.class);
	  job2.setInputFormatClass(TextInputFormat.class);
	  job2.setOutputKeyClass(Text.class);
	  job2.setOutputValueClass(Text.class);
	  job2.setMapOutputValueClass(Text.class);
	  FileInputFormat.setInputPaths(job2, new Path(args[2])); // Output from MapReduce 1 is input to Solution 2 problem
	  FileOutputFormat.setOutputPath(job2, new Path(args[4]));
	  
	  //Job completion status
	  int Solution1Status = (job1.waitForCompletion(true) ? 0: 1);
	  int Solution2Status = (job2.waitForCompletion(true) ? 0: 1);
	  if(Solution1Status == 0 && Solution2Status == 0){
		  return 0;
	  }
	  return 1;
   }

   public static void main(String[] args) throws Exception { 
	   
	   if(args.length != 5) {
		   System.err.println("usage: MRdriver <input-path-to-Movies.dat> <input-path-to-Ratings.dat> <output1-path> <Problem_Statement_1_reducer-Output2> <Problem_Statement_2_reducer-Output2>");
		   System.exit(1);
	   }
	  
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new MRdriver(), args));
   } 
}
