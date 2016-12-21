package lvs.driver;

import java.io.IOException;
import java.util.List;

import lvs.hashjob.HashMapper;
import lvs.hashjob.TwoLevelHashMapper;
import lvs.lpajob.LPAMapper;
import lvs.lpajob.LPAReducer;
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


public class MergeDriver {
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
		job.setJarByClass(StaticALPDriver.class);
		job.setMapperClass(LPAMapper.class);
		job.setReducerClass(LPAReducer.class);
		//job.setNumReduceTasks(50);
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
		job.setJarByClass(StaticALPDriver.class);
		job.setMapperClass(HashMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
	public static Job getHashjob(String inputpath, String outputpath, int partitionNum, int degree)
			throws IOException {

		Configuration conf = new Configuration();
		conf.setInt(Constants.PARTITION_NUMBER, partitionNum);
		conf.setInt(Constants.DEGREE_THRESHOLD, degree);
		FileSystem hdfs = FileSystem.get(conf);
		Path Path = new Path(outputpath);
		if (hdfs.exists(Path)){
			hdfs.delete(Path,true);
		};
		Job job = new Job(conf, "Hash");
		job.setJarByClass(StaticALPDriver.class);
		job.setMapperClass(TwoLevelHashMapper.class);
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
		job.setJarByClass(StaticALPDriver.class);
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
		if(args.length != 6){
			System.err.println("Usage: StaticALP <inputpath> <outputpath> <iteratorNum> <partitionNum> <vertexNum> <degree>");
			System.err.println("<inputpath>: the file input path");
			System.err.println("<outputpath>: the file output path");
			System.err.println("<iteratorNum>: the iterator number of the job");
			System.err.println("<partitionNum>: the number of partitions you want get");
			System.err.println("<vertexNum>: the total vertex number of graph data");
			System.err.println("<degree>: the degree threshold of twolevel partitioning job");
			System.exit(1);//终止JVM，非0表示异常终止
		}
		
	    String inputtemp = null;
	    String outputtemp = null;
		String inputpath = args[0];
		String outputpath = args[1];
		int iteratorNum = Integer.parseInt(args[2]);
		int partitionNum = Integer.parseInt(args[3]);
		int vertexNum = Integer.parseInt(args[4]);
		int degree = Integer.parseInt(args[5]);
		
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
		
		if(iteratorNum == 1){
			//Job hashjob = getHashjob(inputpath,outputpath,partitionNum);
			Job hashjob = getHashjob(inputpath,outputpath,partitionNum,degree);
			hashjob.waitForCompletion(true);
			Job countjob = getCountjob(outputpath,outputpath+"count");
			countjob.waitForCompletion(true);
		}
		if(iteratorNum > 1){
			for(int i=0; i<iteratorNum; i++){
				if(i == 0){
					//Job hashjob = getHashjob(inputpath,outputpath,partitionNum);
					Job hashjob = getHashjob(inputpath,outputpath,partitionNum,degree);
					hashjob.waitForCompletion(true);
					Job countjob = getCountjob(outputpath,outputpath+"count");
					countjob.waitForCompletion(true);
					inputtemp = outputpath;
				}else{
					outputtemp = outputpath + Integer.toString(i);
					Job lpjob = getLPAjob(inputtemp,outputtemp,partitionNum,vertexNum);
					lpjob.waitForCompletion(true);
					inputtemp = outputtemp;
				}
			}
		}
		
		long stop = System.currentTimeMillis();
		System.out.println("the total runtime is: " + (stop-start) +" ms");
	}

}
