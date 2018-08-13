package com.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Fen qu
 * @author zhjzhang
 *
 */
public class TQPartition extends HashPartitioner<Weather, IntWritable>{

	@Override
	public int getPartition(Weather weather, IntWritable value, int numReduceTasks) {
		// TODO Auto-generated method stub
//		return super.getPartition(key, value, numReduceTasks);
		return (weather.getYear() - 1990) % numReduceTasks;
	}

}
