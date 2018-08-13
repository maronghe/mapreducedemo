package com.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS 测试类
 * @author 马荣贺
 *
 */
public class HdfsTest {
	
	public static void main(String[] args) throws IOException {
		FileSystem fs;
		Configuration conf;
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://172.16.140.145:8020");
		
		fs = FileSystem.get(conf);
		
		@SuppressWarnings("deprecation")
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path("/temp/seq"), 
				Text.class,Text.class);
		
		File file = new File("/home/zhjzhang/env/ttt");
		for(File f : file.listFiles()) {
			if(f.isDirectory()) {
				getChildFiles(file,writer);
			}else {
				writer.append(new Text(f.getName()), 
						new Text(FileUtils.readFileToString(f)));
			}
			
		}
	
	}
	
	private FileSystem fs;
	private Configuration conf;
	@Before
	public void begin() throws Exception {
		// read all config in src
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://172.16.140.145:8020");
		fs = FileSystem.get(conf);
	}
	
	@After
	public void end() {
		try {
			//关闭资源
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void mkdir()  {
		Path f = new Path("/testdir");
		try {
			//创建文件夹
			fs.mkdirs(f);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * list 显示文件信息
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	@Test
	public void list() throws FileNotFoundException, IOException {
		Path path = new Path("/zxc");
		FileStatus[] files = fs.listStatus(path);
		for(FileStatus f : files) {
			System.out.println(f.getPath() +" - " + f.getBlockSize() 
				+ " - " + new Date(f.getAccessTime()));
		}
	}
	
	/**
	 * 上传指定文件
	 * @throws Exception
	 */
	@Test
	public void upload() throws Exception {
		FSDataOutputStream outputStream = fs.create(new Path("/temp"));
		FileUtils.copyFile(new File("/home/zhjzhang/env/hosts"), outputStream);
	}
	
	/**
	 * 上传文件夹下多个小文件合并成大文件
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void upload2() throws Exception{
		//处理小文件合并成大文件-减少edits文件大小
		SequenceFile.Writer writer = SequenceFile.createWriter(fs ,conf, new Path("/temp/seq"), Text.class, Text.class );
		File file = new File("/home/zhjzhang/env/ttt");
		for(File f : file.listFiles()) {
			if(f.isDirectory()) {
				//递归判断的所有的文件
				getChildFiles(file,writer);
			}else {
				writer.append(new Text(f.getName()), 
						new Text(FileUtils.readFileToString(f)));
			}
		}
		writer.close();
	}
	
	/**
	 * 获取文件夹下面的文件
	 * @param f
	 * @param writer
	 * @throws IOException
	 */
	public static void getChildFiles(File f,SequenceFile.Writer writer) throws IOException {
		for(File file : f.listFiles()) {
			if(file.isDirectory()) {
				getChildFiles(file,writer);
			}else {
				writer.append(new Text(file.getName()), 
						new Text(FileUtils.readFileToString(file)));
			}
		}
	}
	
	/**
	 *  small file to big file
	 * @throws Exception
	 */
	@Test
	public void download2() throws Exception {
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path("/temp/seq")));
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key,value)) {
			System.out.println(key + " -- " + value);
			System.out.println("============");
		}
		reader.close();
	}
}
