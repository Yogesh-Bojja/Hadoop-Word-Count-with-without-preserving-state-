package edu.rmit.cosc2367.s3789918_S2_2020;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Yogesh_task1 {
	
	// Mapper Class
	public static class AppMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private static final Logger LOG = Logger.getLogger(AppMapper.class);

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The mapper of task-1 running - Yogesh Haresh Bojja, s3789918");
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				

				String str = "Short-Word";
				if(token.length()>=1 && token.length()<=4) {
					str = "Short-Words";
				}
				else if (token.length()>=5 && token.length()<=7) {
					str = "Medium-Words";
				}
				else if(token.length()>=8 && token.length()<=10) {
					str = "Long-Words";
				}
				else {
					str = "Extra-Long-Words";
				}
				
				word.set(str);
				context.write(word, one);
			}
		}
	}

	// Partitioner Class
	public static class AppPartitioner extends
	Partitioner < Text, IntWritable >
	{
		private static final Logger LOG = Logger.getLogger(AppPartitioner.class);
		
	   @Override
	   public int getPartition(Text key, IntWritable value, int numReduceTasks)
	   {
		   // Set log-level to debugging
		   LOG.setLevel(Level.DEBUG);
		   LOG.debug("The partitioner of task-1 running - Yogesh Haresh Bojja, s3789918");
	       
	       if(numReduceTasks == 0)
	       {
	    	   return 0;
	       }
	       
	       if(key.toString().equals("Short-Words") || key.toString().equals("Medium-Words"))
	       {
	    	   return 0;
	       }
	       else if(key.toString().equals("Long-Words"))
	       {
	    	   return 1 % numReduceTasks;
	       }
	       else
	       {
	    	   return 2 % numReduceTasks;
	       }
		}
	}

	// Reducer Class
	public static class AppReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		private static final Logger LOG = Logger.getLogger(AppReducer.class);
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The reducer of task-1 running - Yogesh Haresh Bojja, s3789918");
			int sum = 0;
			// Aggregate values
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word length");
		final Logger LOG = Logger.getLogger(AppReducer.class);
		LOG.setLevel(Level.INFO);
		
		job.setJarByClass(Yogesh_task1.class);
		LOG.info("The mapper of task-1 configured - Yogesh Haresh Bojja, s3789918");
		job.setMapperClass(AppMapper.class);
		job.setPartitionerClass(AppPartitioner.class);
		job.setCombinerClass(AppReducer.class);
		LOG.info("The reducer of task-1 configured - Yogesh Haresh Bojja, s3789918");
		job.setReducerClass(AppReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}

}
