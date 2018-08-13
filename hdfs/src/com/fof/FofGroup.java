package com.fof;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 二度朋友关系Group
 * @author 马荣贺
 *
 */
public class FofGroup  extends WritableComparator{
	
	public FofGroup() {
		super(Text.class,true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text str1 = (Text)a;
		Text str2 = (Text)b;
		int  a1 = str1.toString().split(" ")[0].compareTo(str2.toString().split(" ")[0]);
		return a1;
	}
	
}
