package com.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 天气分区类
 * @author 马荣贺
 *
 */
public class TemperaturePartition extends HashPartitioner<Weather, IntWritable>{

	@Override
	public int getPartition(Weather weather, IntWritable value, int numReduceTasks) {
		return (weather.getYear() - 1990) % numReduceTasks;
	}

}
