package com.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SxtDemo {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//		System.setProperty("HADOOP_USER_NAME", "logan");// config the user name to process hdfs * mapreduce
		
		//default load src core-site.xml & hdfs-site.xml
		System.out.println("Logan: create conf object.");
		Configuration conf = new Configuration();

//		conf.set("fs.defaultFS", "hdfs://n2:8020");
//		conf.set("yarn.resourcemanager.hostname", "n3");
		
		Job job = Job.getInstance();
		// the enter in our application
		job.setJarByClass(SxtDemo.class);
		
		job.setMapperClass(SxtMapper.class);
		job.setReducerClass(SxtReduce.class);

		// stream process , so must is writable and comparable 
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileSystem fs = FileSystem.get(conf);
		Path inPath =  new Path("/tempfiles");
		System.out.println("Logan: create conf object."+ inPath.getName());
		System.out.println(fs.exists(inPath));
		FileInputFormat.addInputPath(job,inPath);
		
		Path outPath = new Path("/tempout");
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		boolean isCompleted = job.waitForCompletion(true);
		System.out.println("Resulut MapReduce... : " + isCompleted);
	}
}
