package com.okair.bigdata;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class TestUpload {

	public static void main(String[] args) throws Exception {
		TestUpload testUpload = new TestUpload();
		testUpload.test2();
	}

	public void test1() throws Exception{
		//指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://bigdata-01:8020");
		conf.set("dfs.client.use.datanode.hostname", "true");
		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//构造一个输入流，从本地读入数据
		InputStream input = new FileInputStream("D:\\pnrphone.log");

		//构造一个输出流 指向HDFS
		OutputStream output = client.create(new Path("/user/Administrator/abc.log"));

		//构造一个缓冲区
		byte[] buffer = new byte[1024];
		//长度
		int len = 0;

		//读入数据
		while((len=input.read(buffer)) > 0){
			//写到输出流中
			output.write(buffer, 0, len);
		}

		output.flush();

		//关闭流
		input.close();
		output.close();
	}
	

	public void test2() throws Exception{
		//指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "hdfs");

		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
	//	conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");
		conf.set("fs.defaultFS", "hdfs://bigdata-01:8020");
		conf.set("dfs.client.use.datanode.hostname", "true");
		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//构造一个输入流，从本地读入数据
		InputStream input = new FileInputStream("D:\\pnrphone.log");

		//构造一个输出流 指向HDFS
		OutputStream output = client.create(new Path("/test/abcd.log"));

		//使用HDFS的一个工具类简化代码
		IOUtils.copyBytes(input, output, 1024);
	}
}




















