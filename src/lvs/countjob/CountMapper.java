package lvs.countjob;

import java.io.IOException;

import lvs.util.Vertex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 对标签进行计数，即统计各分区的顶点数
 * @author lvs
 *
 */
public  class CountMapper extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context
	             ) throws IOException, InterruptedException {
		String vertexinfo = value.toString();
		Vertex vertex = new Vertex();
		vertex.fromString(vertexinfo);
		String label = vertex.getVertexlabel();
		word.set(label);
		context.write(word, one);
   }
}