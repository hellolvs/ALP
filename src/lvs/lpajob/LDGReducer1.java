package lvs.lpajob;

import java.io.IOException;
import java.util.HashMap;

import lvs.util.Constants;
import lvs.util.Vertex;
import lvs.zookeeper.ZookeeperImpl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer类从收到的标签中收益得分最高的作为自己的标签。
 * 输出key值为顶点ID及更新后的标签，Value值为其边表
 * @author lvs
 *
 */
public class LDGReducer1 extends Reducer<Text, Text, Text, Text> {
	
	public static Log LOG = LogFactory.getLog(LDGReducer1.class);
	
	private Configuration conf;
	private int partitionNum;
	private int vertexNum;
	private int capacityBound;//各分区的容量上限
	private ZookeeperImpl zk = new ZookeeperImpl();
	
	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		this.partitionNum = conf.getInt(Constants.PARTITION_NUMBER, 0);
		this.vertexNum = conf.getInt(Constants.VERTEX_NUMBER, 0);
		this.capacityBound = (int)(this.vertexNum/this.partitionNum*(1+Constants.BALANCE_FACTOR));
		try{
			zk.connect(Constants.ZOOKEEPER_QUORUM);		
		}catch(Exception e){
			System.err.println("[startServer]: " + e.getMessage());
		}		
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String vertexkey = key.toString();
		Vertex vertex = new Vertex();
		vertex.setVertexID(vertexkey);
		HashMap<String,Integer> LabelMap = new HashMap<String,Integer>();//存放顶点收到的标签及相应的个数 
		String oldLabel = null;//原标签(原分区ID)
		String newLabel = null;//新标签(新分区ID)
		double maxLabelCount = 0;//收益代价得分

		for (Text value : values) {
			String valueinfo = value.toString();
			Vertex vertextmp = new Vertex();
			vertextmp.fromString(valueinfo);
			
			//统计标签个数
			if (LabelMap.containsKey(vertextmp.getVertexlabel()) ){
				LabelMap.put(vertextmp.getVertexlabel(), LabelMap.get(vertextmp.getVertexlabel()) + 1);
			} else {
				LabelMap.put(vertextmp.getVertexlabel(), 1);
			}

			//添加该顶点对应的值、旧标签和边表
			if (vertextmp.getVertexID().equals(vertexkey)) {
				vertex.setVertexvalue(vertextmp.getVertexvalue());
				vertex.setVertexlabel(vertextmp.getVertexlabel());
				vertex.setEdges(vertextmp.getEdges());
			}
		}

		oldLabel = vertex.getVertexlabel();
		newLabel = oldLabel;
		try{
			if(oldLabel != null){
				maxLabelCount = 0;
				double capacity = Integer.parseInt(new String(zk.getData(Constants.ZOOKEEPER_PATH + "/partition-" + oldLabel, false)));
				if(capacity < capacityBound){
					maxLabelCount =  (capacityBound - capacity) * LabelMap.get(oldLabel);
				}
			}//将当前顶点的收益得分最大值置为原标签的收益得分
			
			//选择收益得分最高的标签作为自己的新标签
			for(String e:LabelMap.keySet()){
				double newLabelCount = 0;
				double capacity = Integer.parseInt(new String(zk.getData(Constants.ZOOKEEPER_PATH + "/partition-" + e, false)));
				if(capacity < capacityBound){
					newLabelCount =  (capacityBound - capacity) * LabelMap.get(oldLabel);
				}
				if(newLabelCount > maxLabelCount){
					maxLabelCount = newLabelCount;
					newLabel = e;
				}
			}
			
			zk.setData(Constants.ZOOKEEPER_PATH + "/partition-" + newLabel, 
					Integer.toString(Integer.parseInt(new String(zk.getData(Constants.ZOOKEEPER_PATH + "/partition-" + newLabel, false)))+1).getBytes() , -1);
			zk.setData(Constants.ZOOKEEPER_PATH + "/partition-" + oldLabel, 
					Integer.toString(Integer.parseInt(new String(zk.getData(Constants.ZOOKEEPER_PATH + "/partition-" + oldLabel, false)))-1).getBytes() , -1);
		}catch(Exception e){
			System.err.println("[zkServer]: " + e.getMessage());
		}
		
		vertex.setVertexlabel(newLabel);
		
		context.write(new Text(vertex.toString()),
				new Text());
	}
	
}