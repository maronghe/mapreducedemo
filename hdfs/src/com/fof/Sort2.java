package com.fof;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 二次排序
 * @author 马荣贺
 * 
 *
 */
public class Sort2 extends WritableComparator{

	public Sort2() {
		super(Text.class,true);
	}
	//tom-world 2
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Text str1 = (Text)a;
		Text str2 = (Text)b;
		int a1 = str1.toString().split(" ")[0].compareTo(str2.toString().split(" ")[0]);
		if(a1 == 0) {
			return  - Integer.compare(Integer.valueOf(str1.toString().split(" ")[1]), 
					Integer.valueOf(str2.toString().split(" ")[1]));
		}
	
		return a1;
	}
	
//	public static void main(String[] args) {
//		String str1 = "tom-world 4";
//		String str2 = "tom-world 3";
//		
//		int a1 = str1.toString().split(" ")[0].compareTo(str2.toString().split(" ")[0]);
//		if(a1 == 0) {
//			System.out.println("qwe");
//			System.out.println(Integer.compare(Integer.valueOf(str1.toString().split(" ")[1]), 
//					Integer.valueOf(str2.toString().split(" ")[1]))); 
//		}else {
//			System.out.println("asd"); 
//			System.out.println(a1);
//		}
//	}

	
}
