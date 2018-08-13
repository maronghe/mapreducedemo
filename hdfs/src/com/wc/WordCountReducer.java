package com.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 * @author 马荣贺
 *
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<LongWritable> iter1,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
		
		long sum = 0l ;
		//累加个数
		for(LongWritable i : iter1) {
			sum += i.get();
		}
		context.write(new Text(arg0), new LongWritable(sum));
	
	}
	
}
