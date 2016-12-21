package lvs.lpajob;

import java.io.IOException;
import java.util.HashMap;

import lvs.util.Constants;
import lvs.util.Vertex;
import lvs.util.HashPartitioner;
import lvs.zookeeper.ZookeeperImpl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer类从收到的标签中收益得分最高的作为自己的标签。
 * 输出key值为顶点ID及更新后的标签，Value值为其边表
 * @author lvs
 *
 */
public class LPAReducer extends Reducer<Text, Text, Text, Text> {
	
	private Configuration conf;
	private int partitionNum;
	private int vertexNum;
	private int capacityBound;//各分区的容量上限
	private ZookeeperImpl zk = new ZookeeperImpl();
	private HashMap<String,Integer> capacityMap = new HashMap<String,Integer>();//存放各个分区的当前容量
	private HashMap<String,Integer> capacityChangeMap = new HashMap<String,Integer>();//存放各个分区顶点数的迁入/迁出量
	
	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		this.partitionNum = conf.getInt(Constants.PARTITION_NUMBER, 0);
		this.vertexNum = conf.getInt(Constants.VERTEX_NUMBER, 0);
		this.capacityBound = (int)(this.vertexNum/this.partitionNum*(1+Constants.BALANCE_FACTOR));
		try{
			zk.connect(Constants.ZOOKEEPER_QUORUM);		
			for(int i=1; i<=partitionNum; i++){
	        	int capacity = Integer.parseInt(new String(zk.getData(Constants.ZOOKEEPER_PATH + "/partition-" + Integer.toString(i), false)));
	        	capacityMap.put(Integer.toString(i), capacity);
	        	capacityChangeMap.put(Integer.toString(i), 0);
	        }
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
		String oldLabel = "0";//原标签(原分区ID)
		String newLabel = "0";//新标签(新分区ID)
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
		
		if((!Integer.toString(0).equals(oldLabel)) && (oldLabel != null)){
			maxLabelCount = 0;
			double capacity = capacityMap.get(oldLabel) + capacityChangeMap.get(oldLabel);
			if(capacity < capacityBound){
				maxLabelCount =  (capacityBound - capacity)*LabelMap.get(oldLabel);
				//maxLabelCount =  Math.sqrt(4*(capacityBound - capacity)*(this.vertexNum/this.partitionNum*Constants.BALANCE_FACTOR))*LabelMap.get(e);
			}
		}//将当前顶点的收益得分最大值置为原标签的收益得分
		
		//选择收益得分最高的标签作为自己的新标签
		for(String e:LabelMap.keySet()){
			if(!Integer.toString(0).equals(e)){
				double newLabelCount = 0;
				double capacity = capacityMap.get(e) + capacityChangeMap.get(e);
				if(capacity < capacityBound){
					newLabelCount =  (capacityBound - capacity)*LabelMap.get(oldLabel);
					//newLabelCount =  Math.sqrt(4*(capacityBound - capacity)*(this.vertexNum/this.partitionNum*Constants.BALANCE_FACTOR))*LabelMap.get(e);
				}
				if(newLabelCount > maxLabelCount){
					maxLabelCount = newLabelCount;
					newLabel = e;
				}
			}
		}
		
		if(Integer.toString(0).equals(newLabel)){
			HashPartitioner<String> hashPartitioner = new HashPartitioner<String>(partitionNum);
			newLabel = Integer.toString(hashPartitioner.getPartitionID(vertexkey));
		}
		
		vertex.setVertexlabel(newLabel);
		
		//更新各分区的迁入/迁出量
		if(!newLabel.equals(oldLabel)){
			if(!Integer.toString(0).equals(oldLabel)){
				capacityChangeMap.put(oldLabel, capacityChangeMap.get(oldLabel)-1);
			}
			capacityChangeMap.put(newLabel, capacityChangeMap.get(newLabel)+1);
		}
		
				
		context.write(new Text(vertex.toString()),
				new Text());
	}
	
	/**
	 * 更新zookeeper所维护的各分区顶点数
	 */
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		try{
			for(int i=1; i<=partitionNum; i++){
				zk.setData(Constants.ZOOKEEPER_PATH + "/partition-" + Integer.toString(i), 
						Integer.toString(Integer.parseInt(new String(zk.getData(Constants.ZOOKEEPER_PATH + "/partition-" + Integer.toString(i), false)))+capacityChangeMap.get(Integer.toString(i))).getBytes() , -1);
			}
		}catch(Exception e){
			System.err.println("[zkServer]: " + e.getMessage());
		}
	}
	
	
}