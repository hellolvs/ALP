package lvs.driver;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import lvs.lpajob.LPAMapper;
import lvs.lpajob.LPAReducer;
import lvs.extractjob.LabelExtractMapper;
import lvs.attachjob.MapSideJoinMapper;
import lvs.util.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DynamicALPDriver {
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
		job.setJarByClass(DynamicALPDriver.class);
		job.setMapperClass(LPAMapper.class);
		job.setReducerClass(LPAReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
	public static Job getLabelExtractjob(String inputpath, String outputpath)
			throws IOException {

		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path Path = new Path(outputpath);
		if (hdfs.exists(Path)){
			hdfs.delete(Path,true);
		};
		Job job = new Job(conf, "LabelExtract");
		job.setJarByClass(DynamicALPDriver.class);
		job.setMapperClass(LabelExtractMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
	public static Job getLabelAttachjob(String oldinputpath, String newinputpath, String outputpath, int partitionNum)
			throws IOException, URISyntaxException {

		Configuration conf = new Configuration();
		conf.setInt(Constants.PARTITION_NUMBER, partitionNum);
		DistributedCache.addCacheFile(new URI(Constants.HDFS_ROOT+oldinputpath), conf);
		FileSystem hdfs = FileSystem.get(conf);
		Path Path = new Path(outputpath);
		if (hdfs.exists(Path)){
			hdfs.delete(Path,true);
		};
		Job job = new Job(conf, "LabelAttach");
		job.setJarByClass(DynamicALPDriver.class);
		job.setMapperClass(MapSideJoinMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(newinputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));
	
		return job;
	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		
		long start = System.currentTimeMillis();
		
		if(args.length != 6){
			System.err.println("Usage: DynamicALP <oldinputpath> <newinputpath> <outputpath> <iteratorNum> <partitionNum> <vertexNum>");
			System.err.println("<oldinputpath>: the file input path of old labeled graph data");
			System.err.println("<newinputpath>: the file input path of new graph data");
			System.err.println("<outputpath>: the file output path");
			System.err.println("<iteratorNum>: the iterator number of the job");
			System.err.println("<partitionNum>: the number of partitions you want get");
			System.err.println("<vertexNum>: the total vertex number of graph data");
			System.exit(1);//终止JVM，非0表示异常终止
		}
		
	    String inputtemp = null;
	    String outputtemp = null;
		String oldinputpath = args[0];
		String newinputpath = args[1];
		String outputpath = args[2];
		int iteratorNum = Integer.parseInt(args[3]);
		int partitionNum = Integer.parseInt(args[4]);
		int vertexNum = Integer.parseInt(args[5]);	
		
		if(iteratorNum == 1){
			Job extractjob = getLabelExtractjob(oldinputpath,outputpath+"labelextract");
			extractjob.waitForCompletion(true);
			Job attachjob = getLabelAttachjob(outputpath+"labelextract/part-m-00000",newinputpath,outputpath,partitionNum);
			attachjob.waitForCompletion(true);
		}
		if(iteratorNum > 1){
			for(int i=0; i<iteratorNum; i++){
				if(i == 0){
					Job extractjob = getLabelExtractjob(oldinputpath,outputpath+"labelextract");
					extractjob.waitForCompletion(true);
					Job attachjob = getLabelAttachjob(outputpath+"labelextract/part-m-00000",newinputpath,outputpath,partitionNum);
					attachjob.waitForCompletion(true);
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
