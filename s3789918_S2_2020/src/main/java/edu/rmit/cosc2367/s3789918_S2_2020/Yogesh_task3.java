package edu.rmit.cosc2367.s3789918_S2_2020;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.rmit.cosc2367.s3789918_S2_2020.Yogesh_task1.AppReducer;

public class Yogesh_task3 {
	
	// Mapper Task
	public static class TokenizerMapper2
	extends Mapper<Object, Text, Text, IntWritable>{

		private Text word = new Text();
		private Map<String, Integer> dict = new HashMap<String, Integer>();
		private static final Logger LOG = Logger.getLogger(TokenizerMapper2.class);
		
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The mapper of task-3 running - Yogesh Haresh Bojja, s3789918");
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				
				// Check if Key already exists
				if(dict.containsKey(token)) {
					// Increment of value
					int sum = dict.get(token) + 1;
					dict.put(token, sum);
				}
				else {
					dict.put(token, 1);
				}

			}
			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			for(String key : dict.keySet()) {
				word.set(key);
				context.write(word, new IntWritable(dict.get(key)));
			}
		}
	}

	// Reducer Task
	public static class IntSumReducer2
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		private static final Logger LOG = Logger.getLogger(IntSumReducer2.class);

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The reducer of task-3 running - Yogesh Haresh Bojja, s3789918");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count 2");
		final Logger LOG = Logger.getLogger(AppReducer.class);
		LOG.setLevel(Level.INFO);
		
		job.setJarByClass(Yogesh_task3.class);
		LOG.info("The mapper of task-3 configured - Yogesh Haresh Bojja, s3789918");
		job.setMapperClass(TokenizerMapper2.class);
		job.setCombinerClass(IntSumReducer2.class);
		LOG.info("The reducer of task-3 configured - Yogesh Haresh Bojja, s3789918");
		job.setReducerClass(IntSumReducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
