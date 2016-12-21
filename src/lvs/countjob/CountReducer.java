package lvs.countjob;

import java.io.IOException;

import lvs.util.Constants;
import lvs.zookeeper.ZookeeperImpl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 对标签进行计数，即统计各分区的顶点数。同时写入zookeeper
 * @author lvs
 *
 */
public  class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	private IntWritable result = new IntWritable();
	private ZookeeperImpl zk = new ZookeeperImpl();
	
	@Override
	public void setup(Context context) {
		try{
			zk.connect(Constants.ZOOKEEPER_QUORUM);		
		}catch(Exception e){
			System.err.println("[startServer]: " + e.getMessage());
		}
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		
		//向zookeeper写入各分区的顶点数
		if(key.toString() != "0"){
			try {
				zk.setData(Constants.ZOOKEEPER_PATH + "/partition-" + key.toString(), Integer.toString(sum).getBytes() , -1);
			} catch (Exception e) {
				System.err.println("[zkServer]: " + e.getMessage());
			}
		}
		
		context.write(key, result);
	}
}
