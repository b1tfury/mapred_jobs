import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Cluster {
	public static  class Map extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		public void map(LongWritable key, Text input, Context context) throws IOException, InterruptedException{
			String value = input.toString();
			int [] setA = new int[9];
			int [] setB = new int[12];
			int [] setC = new int[33];
			int [] setD = new int[51];
			int [] setE = new int[12];
			int [] setF = new int[6];
			int [] setG = new int[8];
			String [] values = value.toString().split("[ABCDEFG]");
		/**	for(int i=1;i<values.length;i++)
			System.out.println(i+" "+values[i]);
			**/
			for(int i =1;i<values.length;i++){
				String [] subValue = values[i].split(",");
				if(i==1){
					for(int j = 0;j<subValue.length;j++)
						setA[Integer.parseInt(subValue[j])] = 1;
					}
					else if(i==2){
						for(int j = 0;j<subValue.length;j++)
							setB[Integer.parseInt(subValue[j])] = 1;
						}
					else if(i==3){
						for(int j = 0;j<subValue.length;j++)
							setC[Integer.parseInt(subValue[j])] = 1;
						}
					else if(i==4){
						for(int j = 0;j<subValue.length;j++)
							setD[Integer.parseInt(subValue[j])] = 1;
						}
					else if(i==5){
						for(int j = 0;j<subValue.length;j++)
							setE[Integer.parseInt(subValue[j])] = 1;
						}
					else if(i==6){
						for(int j = 0;j<subValue.length;j++)
							setF[Integer.parseInt(subValue[j])] = 1;
						}
					else if(i==7){
						for(int j = 0;j<subValue.length;j++)
							setG[Integer.parseInt(subValue[j])] = 1;
						}
			}
			String str = "" ;
			
			for(int i = 1;i<setA.length;i++)
			{
				
					str = str + setA[i]+",";
			}
			for(int i = 1;i<setB.length;i++)
			{
				
					str = str + setB[i]+",";
			}
			for(int i = 1;i<setC.length;i++)
			{
					str = str + setC[i]+",";
			}
			for(int i = 1;i<setD.length;i++)
			{
				
					str = str + setD[i]+",";
			}
			for(int i = 1;i<setE.length;i++){
				
					str = str + setE[i]+",";
			}
			for(int i = 1;i<setF.length;i++){
				
					str = str + setF[i]+",";
			}
			for(int i = 1;i<setG.length;i++){
				
					str = str + setG[i]+",";
			}
		context.write(new Text(str.substring(0, str.length()-1)),NullWritable.get());

	}
	}
	/**
	public static class Reduce extends Reducer<LongWritable, Text, Text, NullWritable>{
	
		public void reduce(LongWritable key , Text value,Context context) throws IOException, InterruptedException{
			context.write(value, NullWritable.get());
		}
		
	}
	**/
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Cluster");
		job.setJarByClass(Cluster.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
	/**	job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		**/
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)? 0:1);
		
	}

}
