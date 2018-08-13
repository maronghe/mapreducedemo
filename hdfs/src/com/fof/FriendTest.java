package com.fof;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 二度朋友关系测试类
 * 需求：按照联系人名字升序 和 联系人关系密度倒叙 排列
 * Solution：一个人的联系表中的所有人，都是二度关系，但是有些人不是真正的二度关系，所以需要去除
 * 两套Mapper Reducer来解决 
 * 第一套MR 用来解决找出真正的二度关系（包括去除非二度关系） 和 按照姓名升序排列
 * 第二套MR 用来按照亲密度倒叙排列
 * @author 马荣贺
 *
 */
public class FriendTest {

	public static void main(String[] args) throws Exception {
		//创建conf对象
		Configuration conf = new Configuration();
		//设置Active NN和Resource Manager
		conf.set("fs.defaultFS", "hdfs://n1:8020");
		conf.set("yarn.resourcemanager.hostname", "n3");
		//获取Job对象
		Job job = Job.getInstance(conf);
		//设置MapOutputKey类型
		job.setMapOutputKeyClass(Text.class);
		//设置设置MapOutputValue类型
		job.setMapOutputValueClass(IntWritable.class);
		//设置mapper
		job.setMapperClass(FofMapper.class);
		//设置Reducer
		job.setReducerClass(FofReducer.class);
		//添加hdfs文件路径
		FileInputFormat.addInputPath(job, new Path("/fof"));
		//设置输出路径
		Path outputPath = new Path("/fofout");
		//获取fs对象，用于判断输出路径是否存在
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath, true);//删除输出路径下所有文件
		}
		//设置输出路径
		FileOutputFormat.setOutputPath(job, outputPath);
		//等待响应结束
		boolean flag = job.waitForCompletion(true);
		System.out.println("job1 : " + flag);

		Job job2 = Job.getInstance(conf);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setSortComparatorClass(Sort2.class);//设置sort 排序
		//		job2.setPartitionerClass(Partition2.class);//设置partition 分区
		job2.setGroupingComparatorClass(FofGroup.class);//设置group 分组
		//		job2.setNumReduceTasks(3);//设置reducer个数

		FileInputFormat.addInputPath(job2, new Path("/fofout"));
		Path p2 = new Path("/fofout2");
		if(fs.exists(p2)) {
			fs.delete(p2,true);
		}
		FileOutputFormat.setOutputPath(job2, p2);

		boolean b2 = job2.waitForCompletion(true);
		System.out.println("job2 : " + b2);
	}
}
