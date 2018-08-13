package com.fof;

/**
 * Utils
 * @author 马荣贺
 *
 */
public class FOFUtils {
	
	public static String format(String f1,String f2) {
		if(f1.compareTo(f2) > 0) {
			return f2 + "-" + f1;
		}
		return f1 + "-" + f2;
	}
}
