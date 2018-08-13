package com.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组类
 * @author 马荣贺
 *
 */
public class TemperatureGroup extends WritableComparator {

	public TemperatureGroup() {
		super(Weather.class , true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		Weather w1 = (Weather) a;
		Weather w2 = (Weather) b;
		
		int r1 = Integer.compare(w1.getYear(), w2.getYear());
		
		if(r1 == 0) {
			int r2 = Integer.compare(w1.getMonth(), w2.getMonth());
			return r2;
		}
		return r1;
	}

}
