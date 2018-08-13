package com.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 * 二度朋友关系Mapper
 * @author 马荣贺
 *
 */
public class FofMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//截取
		String[] strs = StringUtils.split(value.toString(), ' ');
		//同好友列表的人，都是二度关系
		for (int i = 1; i < strs.length; i++) {
			String str  = StringUtils.format(strs[0], strs[i]);
			//不是真正的二度关系
			context.write(new Text(str), new IntWritable(0));
			for (int j = i + 1; j < strs.length; j++) {
				String str2 = StringUtils.format(strs[i], strs[j]);
				context.write(new Text(str2), new IntWritable(1));
			}
		}
	}
	
}
