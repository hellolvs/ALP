package lvs.lpajob;


import java.io.IOException;

import lvs.util.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



/**
 * Mapper类向邻居顶点发送自己的ID和标签。
 * 输出的Key值为各邻居顶点的ID，Value值为各邻居顶点收到的源顶点ID及其标签（向自身发送的Value值还有边表）
 * @author lvs
 *
 */
public class LPAMapper extends Mapper<LongWritable, Text, Text, Text> {	
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String vertexinfo = value.toString();
		Vertex vertex = new Vertex();
		vertex.fromString(vertexinfo); 
		
		if(vertex.getEdges() != null){
			String[] edgearray = vertex.getedgearary();
			//向邻居顶点发送标签
			for (String anEdgearray : edgearray) {
				String[] IDandvalue = anEdgearray.split(":");
				context.write(new Text(IDandvalue[0]),
						new Text(vertex.getIDvalueandLabel()));
			}
		}
		//向自身发送全部顶点信息
		context.write(new Text(vertex.getVertexID()),
				new Text(vertex.toString()));
	}
}