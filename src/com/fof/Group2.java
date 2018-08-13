package com.fof;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Group2  extends WritableComparator{
	
	public Group2() {
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
