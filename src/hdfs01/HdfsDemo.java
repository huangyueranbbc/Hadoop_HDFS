package hdfs01;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HdfsDemo {
	FileSystem fs;
	private Configuration conf;

	@Before
	public void begin() {
		System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin");
		conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master:9000");

		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@After
	public void end() {
		try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 创建目录
	 * 
	 * @throws IOException
	 */
	@Test
	public void mkdir() throws IOException {
		Path path = new Path("/test1");
		boolean flag = fs.mkdirs(path);
		System.out.println(flag);
		Assert.assertTrue("创建失败", flag);
	}

	/**
	 * 上传
	 * 
	 * @throws IOException
	 */
	@Test
	public void upload() throws IOException {
		Path srcPath = new Path("D://test.txt");
		Path dstPath = new Path("/test1");
		fs.copyFromLocalFile(false, srcPath, dstPath);
	}

	/**
	 * 查看
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	@Test
	public void ls() throws FileNotFoundException, IOException {
		Path path = new Path("/");
		FileStatus[] listStatus = fs.listStatus(path);

		for (FileStatus fs : listStatus) {
			System.out.println(fs.toString());
		}
	}

	/**
	 * 下载
	 * 
	 * @throws IOException
	 */
	@Test
	public void download() throws IOException {
		fs.copyToLocalFile(new Path("/test1/test.txt"), new Path("d:/xxx.txt"));
	}

	/**
	 * @throws IOException
	 * @category 合并小文件 上传合并后的大文件
	 */
	@Test
	public void upload2() throws IOException {
		Path seqFile = new Path("/test/seqFile3"); // 合并并存储的文件
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(seqFile), Writer.keyClass(Text.class),
				Writer.valueClass(Text.class), Writer.compression(CompressionType.NONE));

		File file = new File("D://test");
		for (File f : file.listFiles()) {
			// 通过writer向文档中写入记录
			writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
		}

	}

	/**
	 * 下载合并后的大文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void download2() throws IOException {
		Path path = new Path("/test/seqFile2");
		Reader reader = new Reader(fs, path, conf);
		Text key = new Text();
		Text value = new Text();
		while (reader.next(key, value)) {
			System.out.println(key);
			System.out.println(value);
			System.out.println("======================");
		}
	}

}
