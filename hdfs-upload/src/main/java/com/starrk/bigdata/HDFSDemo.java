package com.starrk.bigdata;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;


public class HDFSDemo {

	public static void main(String[] args) throws Exception {
		HDFSDemo h =new HDFSDemo();
		h.testDownload();
	}

	public void testDownload() throws Exception{
		//数据下载
		//指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "hive");

		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://101.200.57.237:8020");

		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//构造一个输入流，从HDFS中读取数据
		InputStream input = client.open(new Path("/user/hive/warehouse/lsz_test/hhh.csv"));

		//构造一个输出流，输出到本地的目录
		OutputStream output = new FileOutputStream("C:\\Users\\Administrator\\Desktop\\hhh.csv");

		//使用工具类
		IOUtils.copyBytes(input, output, 1024);
	}
	

	public void testDataNode()  throws Exception{
		//获取DataNode的信息（伪分布的环境）
		//指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");

		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

		//创建一个HDFS客户端
		//FileSystem client = FileSystem.get(conf);
		DistributedFileSystem fs = (DistributedFileSystem) FileSystem.get(conf);

		//获取数据节点的信息: Stats ---> 统计信息
		DatanodeInfo[] list = fs.getDataNodeStats();
		for(DatanodeInfo info:list){
			System.out.println(info.getHostName()+"\t"+ info.getName());
		}

		fs.close();
	}
	

	public void testFileBlockLocation() throws Exception{
		//获取数据块的信息
		//指定当前的Hadoop的用户
		System.setProperty("HADOOP_USER_NAME", "root");

		//配置参数：指定NameNode地址
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.157.111:9000");

		//创建一个客户端
		FileSystem client = FileSystem.get(conf);

		//获取文件的status信息
		FileStatus fileStatus = client.getFileStatus(new Path("/folder111/a.tar.gz"));

		//获取文件的数据块信息（数组）
		BlockLocation[] locations = client.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		/*
		 * 伪分布的环境，数据块的冗余度是：1
		 */
		for(BlockLocation blk:locations){
			System.out.println(Arrays.toString(blk.getHosts()) + "\t" + Arrays.toString(blk.getNames()));
		}

		client.close();
	}
}





























