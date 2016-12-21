package lvs.extractjob;


import java.io.IOException;

import lvs.util.Vertex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * 对划分后数据抽取标签。
 * @author lvs
 *
 */
public class LabelExtractMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String vertexinfo = value.toString();
		Vertex vertex = new Vertex();
		vertex.fromString(vertexinfo);
		
		context.write(new Text(vertex.getVertexID()),new Text(vertex.getVertexlabel()));
	}
}