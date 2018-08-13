package com.tq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 天气Reducer
 * @author RongHeMaRongHe
 *
 */
public class TemperatureReduce extends Reducer<Weather, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Weather weather, Iterable<IntWritable> iterable,Context context) throws IOException, InterruptedException {
		
		int flag = 0 ;
		for (IntWritable i : iterable) {
			flag ++ ;
			if(flag > 2) {
				break;
			}
			
			String str = weather.getYear() + "-" + weather.getMonth() + "-" + weather.getDay()  
					+ "\t" + weather.getWd();
			
			context.write(new Text(str), NullWritable.get());
		}
		
	}

	
}
