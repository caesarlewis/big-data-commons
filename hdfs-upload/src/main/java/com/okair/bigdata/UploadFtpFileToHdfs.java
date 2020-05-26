package com.okair.bigdata;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class UploadFtpFileToHdfs {
    public static void main(String[] args) throws IOException {
        System.out.println("开始执行");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata-01:8020");
     //   conf.set("dfs.client.use.datanode.hostname", "true");
   //     System.out.println("参数1为："+args[0]+"参数2为："+args[1]);

     //   loadFromFtpToHdfs("47.93.41.92", "okair", "123456", args[0], args[1], conf);
        loadFromFtpToHdfs("47.93.41.92", "okair", "123456", args[0], args[1], conf);
        System.out.println("结束");
    }

    /**
     *
     * loadFromFtpToHdfs:将数据从ftp上传到hdfs上. <br/>
     *
     * @author qiyongkang
     * @param ip
     * @param username
     * @param password
     * @param filePath
     * @param outputPath
     * @param conf
     * @return
     * @since JDK 1.6
     */
    private static boolean loadFromFtpToHdfs(String ip, String username, String password, String filePath,
                                             String outputPath, Configuration conf) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        FTPClient ftp = new FTPClient();
        InputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        boolean flag = true;

            ftp.connect(ip);
            ftp.login(username, password);
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
            ftp.setControlEncoding("UTF-8");
            int reply = ftp.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftp.disconnect();
            }
            FTPFile[] files = ftp.listFiles(filePath);
            FileSystem hdfs = FileSystem.get(conf);
            inputStream = ftp.retrieveFileStream(filePath);
            outputStream = hdfs.create(new Path(outputPath));

            //构造一个缓冲区
            byte[] buffer = new byte[1024];
            //长度
            int len = 0;

            //读入数据
            while((len=inputStream.read(buffer)) > 0){
                //写到输出流中
                outputStream.write(buffer, 0, len);
            }

            outputStream.flush();

            outputStream.close();
            inputStream.close();

//            IOUtils.copyBytes(inputStream, outputStream, conf, false);
//            if (inputStream != null) {
//                inputStream.close();
//                ftp.completePendingCommand();
//            }
//            for (FTPFile file : files) {
//                if (!(file.getName().equals(".") || file.getName().equals(".."))) {
//                    inputStream = ftp.retrieveFileStream(filePath + file.getName());
//                    outputStream = hdfs.create(new Path(outputPath + file.getName()));
//                    IOUtils.copyBytes(inputStream, outputStream, conf, false);
//                    if (inputStream != null) {
//                        inputStream.close();
//                        ftp.completePendingCommand();
//                    }
//                }
//            }
            ftp.disconnect();

        return flag;
    }
}
