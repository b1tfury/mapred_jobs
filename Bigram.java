package cs246H;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Bigram {
	
	public static class Map extends Mapper<LongWritable, Text, Text,IntWritable>{
		private final static IntWritable one  = new IntWritable(1);
		private Text bigram = new Text();
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String [] values = value.toString().split("\\s+");
			String  previous = null;
			for(String word:values){
				if(word.length() > 0){
					if(previous!=null){
				bigram.set(previous+" "+word);
				context.write(bigram, one);
				}
				}
				previous = word;
		}
	}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private String bigram = null;
		private int max =0;
		public  void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum =0;
			for(IntWritable value :values){
				sum+=value.get();
			}
			if(sum>max)
			{
				bigram = key.toString();
				max = sum;
				context.write(new Text(bigram),new IntWritable(max));
			}
			
			}
			
		}
	
	
	
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "bigram");
		job.setJarByClass(Bigram.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true) ? 0:1);
		
		
	}

}

