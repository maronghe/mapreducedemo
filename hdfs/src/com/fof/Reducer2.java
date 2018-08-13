package com.fof;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, NullWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text text, Iterable<NullWritable> iterable,Context context) throws IOException, InterruptedException {
		String str = text.toString();
		String[] strs = StringUtils.split(str, ' ');
		context.write(new Text(strs[0]), new IntWritable(Integer.valueOf(strs[1])));
	}
}
