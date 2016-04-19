import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PhaseThree {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text outKey = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			 String[] wordAndCounters = value.toString().split("\t");
		     String[] nandCaptialN = wordAndCounters[1].split(",");
		     context.write(new Text(wordAndCounters[0]), new Text("1,"+nandCaptialN[0] + "," + nandCaptialN[1]));
		        
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
        HashMap<String, String> tempFrequencies = new HashMap<String, String>();
        TreeSet <String> documents = new TreeSet<String>();
        
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	
	        int noDocsWordAppeared = 0;
	        String nValue="",NValue="";
	        
	        documents.add(key.toString().split(",")[1]);
	        for (Text val : values) {
	        	
	        	if(noDocsWordAppeared==0){
	        		String[] documentAndFrequencies = val.toString().split(",");
	        		nValue=documentAndFrequencies[1];
	        		NValue=documentAndFrequencies[2];
	        	}
	            noDocsWordAppeared++;
	        }
	 
           
           
        tempFrequencies.put(key.toString(), noDocsWordAppeared+","+nValue+","+NValue) ;   
	    context.write(key, new Text(noDocsWordAppeared+","+nValue+","+NValue));      
	        
	}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			for(String s: tempFrequencies.keySet()){
				
				String[] values=tempFrequencies.get(s).split(",");
				
				double mValue= Double.parseDouble(values[0]);
				double nValue= Double.parseDouble(values[1]);
				double NValue= Double.parseDouble(values[2]);
				double tf=nValue/NValue;
				
				int numberOfDocumentsInCorpus = documents.size();
				
			        
		            double idf = (double) numberOfDocumentsInCorpus / mValue;
		            double tfIdf = numberOfDocumentsInCorpus == (int)mValue ?
		                    tf : tf * (Math.log(idf) / Math.log(2));
		        NumberFormat formatter = new DecimalFormat("#0.00");          
				context.write(new Text(s), new Text(""+formatter.format(tfIdf))); 
			}
		}
		
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PhaseThree");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(PhaseThree.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
