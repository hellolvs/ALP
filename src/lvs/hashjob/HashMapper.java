package lvs.hashjob;


import java.io.IOException;

import lvs.util.Vertex;
import lvs.util.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * 初始Hash划分，对顶点添加标签。
 * @author lvs
 *
 */
public class HashMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Configuration conf;
	private int partitionNum;
	
	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		this.partitionNum = conf.getInt(Constants.PARTITION_NUMBER, 0);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String vertexinfo = value.toString();
		Vertex vertex = new Vertex();
		vertex.fromString(vertexinfo,this.partitionNum);		
		
		Text text = new Text();
		text.set(vertex.toString());
		context.write(text,new Text());
	}
}