package com.tq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TQClient {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		System.out.println("enter");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://n1:8020");
		Job job = Job.getInstance(conf);
		job.setPartitionerClass(TQPartition.class);
		job.setSortComparatorClass(TQSort.class);
		job.setGroupingComparatorClass(TQGroup.class);
		
		job.setMapOutputKeyClass(Weather.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TQMapper.class);
		job.setReducerClass(TQReduce.class);
		
		job.setNumReduceTasks(2);
		
		FileInputFormat.addInputPath(job, new Path("/weather"));
		FileOutputFormat.setOutputPath(job, new Path("/weatherout"));
		
		System.out.println("success ::::+++++++++=========>>>>>>>>>" + job.waitForCompletion(true));
		
	}
}
