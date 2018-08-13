package com.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] strs = StringUtils.split(value.toString(), ' ');
		
		for (int i = 1; i < strs.length; i++) {
			String str  = FoF.format(strs[0], strs[i]);
			context.write(new Text(str), new IntWritable(0));
			for (int j = i + 1; j < strs.length; j++) {
				String str2 = FoF.format(strs[i], strs[j]);
				context.write(new Text(str2), new IntWritable(1));
			}
		}
	}
	
}
