package com.fof;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendClient {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://n1:8020");
		conf.set("yarn.resourcemanager.hostname", "n3");
		Job job = Job.getInstance(conf);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(FofMapper.class);
		job.setReducerClass(FofReducer.class);
		
		FileInputFormat.addInputPath(job, new Path("/fof"));
		Path outputPath = new Path("/fofout");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean flag = job.waitForCompletion(true);
		System.out.println("success ::::" + flag);
		
		Job job2 = Job.getInstance(conf);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setSortComparatorClass(Sort2.class);
//		job2.setPartitionerClass(Partition2.class);
		job2.setGroupingComparatorClass(Group2.class);
//		job2.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job2, new Path("/fofout"));
		Path p2 = new Path("/fofout2");
		if(fs.exists(p2)) {
			fs.delete(p2,true);
		}
		FileOutputFormat.setOutputPath(job2, p2);
		
		boolean b2 = job2.waitForCompletion(true);
		System.out.println("job2 : " + b2);
	}
}
