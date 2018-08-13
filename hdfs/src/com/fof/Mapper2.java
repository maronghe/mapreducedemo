package com.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
		cat-hadoop 2
		cat-hello 2
		cat-mr 1
		cat-world 1
		hadoop-hello 3
		hadoop-mr 1
		hive-tom 3
		mr-tom 1
		mr-world 2
		tom-world 2
	 */
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(value.toString(),' ');
		String[] str2 = StringUtils.split(strs[0],'-');
		context.write(new Text(str2[0]+"-"+str2[1]+" "+Integer.valueOf(strs[1])), new IntWritable(Integer.valueOf(strs[1])));
		context.write(new Text(str2[1]+"-"+str2[0]+" "+Integer.valueOf(strs[1])), new IntWritable(Integer.valueOf(strs[1])));
	}
	
}
