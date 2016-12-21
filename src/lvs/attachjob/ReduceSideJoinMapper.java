package lvs.attachjob;

import lvs.util.Vertex;
import lvs.util.CombineValues;

import java.io.IOException;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;   

/**
 * 使用reduce端左外连接操作对新数据附加旧数据划分得到的标签
 * @author lvs
 *
 */
public class ReduceSideJoinMapper extends Mapper<Object, Text, Text, CombineValues> {
	
	private CombineValues combineValues = new CombineValues();   
    private Text flag = new Text();   
    private Text joinKey = new Text();   
    private Text secondPart = new Text();   
    @Override 
    protected void map(Object key, Text value, Context context)   
            throws IOException, InterruptedException {   
        //获得文件输入路径   
        String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
        //数据来自新数据文件（文件名以0结尾）,标志即为"1"
        if(pathName.endsWith("0")){
            String[] valueItems = value.toString().split("\t");
            //过滤格式错误的记录   
            if(valueItems.length != 2){
                return;
            }
            flag.set("1");   
            joinKey.set(valueItems[0].trim());
            secondPart.set(valueItems[1].trim());
            combineValues.setFlag(flag);
            combineValues.setJoinKey(joinKey);
            combineValues.setSecondPart(secondPart);
            context.write(combineValues.getJoinKey(), combineValues);
        }
        //数据来自于旧数据文件，标志即为"0"   
        else {   
        	String vertexinfo = value.toString();
    		Vertex vertex = new Vertex();
    		vertex.fromString(vertexinfo);
            flag.set("0");   
            joinKey.set(vertex.getVertexID());   
            secondPart.set(vertex.toString());   
            combineValues.setFlag(flag);   
            combineValues.setJoinKey(joinKey);   
            combineValues.setSecondPart(secondPart);   
            context.write(combineValues.getJoinKey(), combineValues);   
        }   
    } 
    
}
