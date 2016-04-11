import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ProgramOne_2 {


	/**
	 * WordFrequenceInDocMapper implements the Job 1 specification for the TF-IDF algorithm
	 */
	public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	 
		//Map 
		//Intput : (document, each line contents)
		//Output : (document, 1)
	 
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) {
	            word.set(tokenizer.nextToken());
	            context.write(word, one);
	        }
	    }
	}
	
	//Reduce
	//Input : output from the mapper and 'we will be summing all the words in the document'
	//Output : (document, n)
	
	public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	    
		static int max = 0; 
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	 
	        int sum = 0;
	        for (IntWritable val : values) {
	            sum += val.get();
	        }
	        //write the key and the adjusted value (removing the last comma)
	        if(sum > max){
	        	context.write(key, new IntWritable(sum));
	        }
	        	
	    }
	}
	
	//	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
		Job job = new Job(conf, "ProgramOne Job Smallest File");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setJarByClass(ProgramOne_2.class);
        job.setMapperClass(Map1.class);
        //job.setReducerClass(Reduce1.class);
//        job.setCombinerClass(Reduce1.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
        	
	}
}
