
package cs246H;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AllCap {
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		public void map(LongWritable key,Text value,Context context)
				throws 	IOException,InterruptedException{	
			value.set(value.toString().toUpperCase());
			context.write(key, value);
			
		}
	}
	
	public static class Reduce extends Reducer<LongWritable, Iterable<Text>, Text, NullWritable>{
		public void reduce(LongWritable key,Iterable<Text> values,Context context)
		throws IOException,InterruptedException{
			for(Text val:values){
				context.write(val, NullWritable.get());
			}
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "allcap");
		job.setJarByClass(AllCap.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)? 0:1);
		
		
		
		
		
		
	}
}
