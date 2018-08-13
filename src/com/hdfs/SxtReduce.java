package com.hdfs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SxtReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> iter1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		System.out.println("reduce.....");
		long sum = 0l ;
		for(LongWritable i : iter1) {
			sum += i.get();
		}
		context.write(new Text(arg0), new LongWritable(sum));
	
	}
	
}
