package com.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * 二度朋友关系Reducer
 * @author 马荣贺
 *
 */
public class FofReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,Context context) throws IOException, InterruptedException {
		boolean flag = true;
		int sum = 0;
		for(IntWritable i : iterable) {
			//去除非二度关系
			if(i.get() == 0) {
				flag = false;
				break;
			}
			sum ++;
		}
		if(flag) {
			context.write(new Text(text.toString() + " " + sum), NullWritable.get());
		}
	}
}
