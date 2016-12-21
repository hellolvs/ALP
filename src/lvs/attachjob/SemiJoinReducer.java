package lvs.attachjob;

import lvs.util.Vertex;
import lvs.util.CombineValues;
import lvs.util.Constants;
import lvs.util.HashPartitioner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 

/**
 * 使用半连接操作对新数据附加旧数据划分得到的标签
 * @author lvs
 *
 */
public class SemiJoinReducer extends Reducer<Text, CombineValues, Text, Text> {
	
	private Configuration conf;
	private int partitionNum;
	
	@Override
	public void setup(Context context) {
		this.conf = context.getConfiguration();
		this.partitionNum = conf.getInt(Constants.PARTITION_NUMBER, 0);
	}
	
    /**   
     * 一个分组调用一次reduce函数   
     */ 
    @Override 
    protected void reduce(Text key, Iterable<CombineValues> value, Context context)   
            throws IOException, InterruptedException {     
        
    	Vertex vertex = new Vertex();  
    	String label = Integer.toString(0);
    	boolean isLeftKey = false;
        for(CombineValues cv : value){       
            if("0".equals(cv.getFlag().toString().trim())){   
                vertex.fromString(cv.getSecondPart().toString()); 
                isLeftKey = true;
            }    
            else if("1".equals(cv.getFlag().toString().trim())){   
                label = cv.getSecondPart().toString();  
            }   
        }
        
        if(isLeftKey){
        	if(!label.equals(Integer.toString(0))){
        		vertex.setVertexlabel(label);
        	}else{
        		HashPartitioner<String> hashPartitioner = new HashPartitioner<String>(partitionNum);
    			Integer partitionID = hashPartitioner.getPartitionID(vertex.getVertexID());
    			vertex.setVertexlabel(partitionID.toString());
        	}
        	context.write(new Text(vertex.toString()),new Text());
        }
    }
    
}