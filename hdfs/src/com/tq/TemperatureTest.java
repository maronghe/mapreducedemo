package com.tq;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 天气
 * @author RongHeMaRongHe
 *
 */
public class TemperatureTest {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://n1:8020");
		Job job = Job.getInstance(conf);
		job.setPartitionerClass(TemperaturePartition.class);
		job.setSortComparatorClass(TemperatureSort.class);
		job.setGroupingComparatorClass(TemperatureGroup.class);
		 
		job.setMapOutputKeyClass(Weather.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReduce.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path("/weather"));
		FileOutputFormat.setOutputPath(job, new Path("/weatherout"));
		
		System.out.println("Result : " + job.waitForCompletion(true));
		
	}
}
