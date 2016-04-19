import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PhaseTwo {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] wordAndDocCounter = value.toString().split("\t");
			String[] keyArr = wordAndDocCounter[0].split(",");// key array
																// contains word
																// and doc name
			context.write(new Text(keyArr[1]), new Text(keyArr[0] + "," + wordAndDocCounter[1]));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sumOfWordsInDocument = 0;
			HashMap<String, Integer> tempCounter = new HashMap<String, Integer>();

			for (Text val : values) {
				String[] wordCounter = val.toString().split(",");
				tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
				sumOfWordsInDocument += Integer.parseInt(val.toString().split(",")[1]);
			}
			for (String wordKey : tempCounter.keySet()) {
				context.write(new Text(wordKey + "," + key.toString()),
						new Text(tempCounter.get(wordKey) + "," + sumOfWordsInDocument));
			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "PhaseTwo");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(PhaseTwo.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
