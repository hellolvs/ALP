package lvs.attachjob;

import lvs.util.Constants;
import lvs.util.HashPartitioner;
import lvs.util.Vertex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 使用map端左外连接操作对新数据附加旧数据划分得到的标签
 * @author lvs
 *
 */
public class MapSideJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	 // 用于缓存label文件中的数据
    private Map<String, String> labelMap = new HashMap<String, String>();
    private Configuration conf;
	private int partitionNum;

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
    	this.conf = context.getConfiguration();
    	this.partitionNum = conf.getInt(Constants.PARTITION_NUMBER, 0);
        try {
            // 从当前作业中获取要缓存的文件
            URI[] uri = DistributedCache.getCacheFiles(context
                    .getConfiguration());
            String line = null;
            FileSystem fs = FileSystem.get(URI.create(Constants.HDFS_ROOT) , context.getConfiguration());
            FSDataInputStream in = fs.open(new Path(uri[0].getPath()));
            BufferedReader br=new BufferedReader(new InputStreamReader(in));
            while ((line = br.readLine()) != null) {
            	labelMap.put(line.split("\t")[0].trim(),
                        line.split("\t")[1].trim());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String vertexinfo = value.toString();
		Vertex vertex = new Vertex();
		vertex.fromString(vertexinfo); 

        // map join
        if (labelMap.containsKey(vertex.getVertexID())) {
            vertex.setVertexlabel(labelMap.get(vertex.getVertexID()));
        }else{
        	HashPartitioner<String> hashPartitioner = new HashPartitioner<String>(partitionNum);
			Integer partitionID = hashPartitioner.getPartitionID(vertex.getVertexID());
			vertex.setVertexlabel(partitionID.toString());
        }
        
        context.write(new Text(vertex.toString()),new Text());
    }

}
