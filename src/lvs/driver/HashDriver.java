package lvs.driver;

import java.io.IOException;


import lvs.hashjob.HashMapper;
import lvs.util.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HashDriver{
	
	public static Job getHashjob(String inputpath, String outputpath, int partitionNum)
			throws IOException {

		Configuration conf = new Configuration();
		conf.setInt(Constants.PARTITION_NUMBER, partitionNum);
		FileSystem hdfs = FileSystem.get(conf);
		Path Path = new Path(outputpath);
		if (hdfs.exists(Path)){
			hdfs.delete(Path,true);
		};
		Job job = new Job(conf, "Hash");
		job.setJarByClass(HashDriver.class);
		job.setMapperClass(HashMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
		
		long start = System.currentTimeMillis();
		/**
		if(args.length != 5){
			System.err.println("Usage: StaticALP <inputpath> <outputpath> <iteratorNum> <partitionNum> <vertexNum>");
			System.err.println("<inputpath>: the file input path");
			System.err.println("<outputpath>: the file output path");
			System.err.println("<iteratorNum>: the iterator number of the job");
			System.err.println("<partitionNum>: the number of partitions you want get");
			System.err.println("<vertexNum>: the total vertex number of graph data");
			System.exit(1);//终止JVM，非0表示异常终止
		}
		*/
		if(args.length != 3){
			System.err.println("Usage: Hash <inputpath> <outputpath> <partitionNum>");
			System.err.println("<inputpath>: the file input path");
			System.err.println("<outputpath>: the file output path");
			System.err.println("<partitionNum>: the number of partitions you want get");
			System.exit(1);//终止JVM，非0表示异常终止
		}
		
		String inputpath = args[0];
		String outputpath = args[1];
		int partitionNum = Integer.parseInt(args[2]);

		Job hashjob = getHashjob(inputpath,outputpath,partitionNum);
		hashjob.waitForCompletion(true);
		
		long stop = System.currentTimeMillis();
		System.out.println("the total runtime is: " + (stop-start) +" ms");
	}

}
