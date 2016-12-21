package lvs.hashjob;


import java.io.IOException;

import lvs.util.Vertex;
import lvs.util.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * 两阶段划分，首先对度大的顶点进行Hash划分。
 * @author lvs
 *
 */
public class TwoLevelHashMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Configuration conf;
	private int partitionNum;
	private int degree;
	
	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		this.partitionNum = conf.getInt(Constants.PARTITION_NUMBER, 0);
		this.degree = conf.getInt(Constants.DEGREE_THRESHOLD, 0);
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String vertexinfo = value.toString();
		Vertex vertex = new Vertex();
		vertex.fromString(vertexinfo,this.partitionNum,this.degree);
		
		Text text = new Text();
		text.set(vertex.toString());
		context.write(text,new Text());
	}
}