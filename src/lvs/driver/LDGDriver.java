package lvs.driver;

import java.io.IOException;
import java.util.List;

import lvs.hashjob.HashMapper;
import lvs.lpajob.LDGReducer;
import lvs.lpajob.LDGReducer1;
import lvs.lpajob.LPAMapper;
import lvs.countjob.CountMapper;
import lvs.countjob.CountReducer;
import lvs.util.Constants;
import lvs.zookeeper.ZookeeperImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LDGDriver {
	public static Job getLPAjob(String inputpath, String outputpath, int partitionNum, int vertexNum)
			throws IOException {

		Configuration conf = new Configuration();
		conf.setInt(Constants.PARTITION_NUMBER, partitionNum);
		conf.setInt(Constants.VERTEX_NUMBER, vertexNum);
		FileSystem hdfs = FileSystem.get(conf);
		Path Path = new Path(outputpath);
		if (hdfs.exists(Path)){
			hdfs.delete(Path,true);
		};
		Job job = new Job(conf, "LPA");
		job.setJarByClass(LDGDriver.class);
		job.setMapperClass(LPAMapper.class);
		job.setReducerClass(LDGReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
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
		job.setJarByClass(LDGDriver.class);
		job.setMapperClass(HashMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
	public static Job getCountjob(String inputpath, String outputpath)
			throws IOException {

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path Path = new Path(outputpath);
		if (hdfs.exists(Path)){
			hdfs.delete(Path,true);
		};
		Job job = new Job(conf, "Count");
		job.setJarByClass(LDGDriver.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
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
		if(args.length != 4){
			System.err.println("Usage: LDG <inputpath> <outputpath> <partitionNum> <vertexNum>");
			System.err.println("<inputpath>: the file input path");
			System.err.println("<outputpath>: the file output path");
			System.err.println("<iteratorNum>: the iterator number of the job");
			System.err.println("<partitionNum>: the number of partitions you want get");
			System.err.println("<vertexNum>: the total vertex number of graph data");
			System.err.println("<degree>: the degree threshold of twolevel partitioning job");
			System.exit(1);//终止JVM，非0表示异常终止
		}
		
		String inputpath = args[0];
		String outputpath = args[1];
		int partitionNum = Integer.parseInt(args[2]);
		int vertexNum = Integer.parseInt(args[3]);
		
		try{
			ZookeeperImpl zk = new ZookeeperImpl();
			zk.connect(Constants.ZOOKEEPER_QUORUM);
			if (zk.equaltoStat(Constants.ZOOKEEPER_PATH, false)) {
	        	List<String> children = zk.getChildren(Constants.ZOOKEEPER_PATH, false);
	            if (children.size() > 0) {
	              for (String child : children) {
	                zk.delete(Constants.ZOOKEEPER_PATH + "/" + child, -1);
	              }
	            }
	            zk.delete(Constants.ZOOKEEPER_PATH, -1);
	        }
			zk.createPersistent(Constants.ZOOKEEPER_PATH,null);
	        for(int i=1; i<=partitionNum; i++){
	        	zk.createPersistent(Constants.ZOOKEEPER_PATH + "/partition-" + Integer.toString(i), Integer.toString(0).getBytes());
	        }
	        zk.close();
		}catch (Exception e) {
			System.err.println("[startServer]: " + e.getMessage());
		}		
		
		Job hashjob = getHashjob(inputpath,outputpath + "hash",partitionNum);
		hashjob.waitForCompletion(true);
		Job countjob = getCountjob(outputpath + "hash",outputpath+"hashcount");
		countjob.waitForCompletion(true);

		Job lpjob = getLPAjob(outputpath + "hash",outputpath,partitionNum,vertexNum);
		lpjob.waitForCompletion(true);
	
		long stop = System.currentTimeMillis();
		System.out.println("the total runtime is: " + (stop-start) +" ms");
	}

}
