package com.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 1996:01:09 22:01:33-15c
 * 
 * @author zhjzhang
 *
 */
public class Weather implements WritableComparable<Weather> {

	private int year;
	private int month;
	private int day;
	private int wd;

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
	}

	@Override
	public int compareTo(Weather weather) {
		int c1 = Integer.compare(this.year, weather.getYear());
		if (c1 == 0) {
			int c2 = Integer.compare(this.month, weather.getMonth());
			if (c2 == 0) {
				int c3 = Integer.compare(this.wd, weather.getWd());
				return c3;
			}
			return c2;
		}
		return 0;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

}
