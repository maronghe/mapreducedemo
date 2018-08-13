package com.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;


/**
 * Mapper
 * @author 马荣贺
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text , LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		
		String[] strs = StringUtils.split(str,' ');
		
		for(String s : strs) {
			if(!"".equals(s))
				context.write(new Text(s), new LongWritable(1l));
		}
		
	}

	
	
}
