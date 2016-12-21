package lvs.attachjob;

import lvs.util.Vertex;
import lvs.util.CombineValues;
import lvs.util.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;   

/**
 * 使用半连接操作对新数据附加旧数据划分得到的标签
 * @author lvs
 *
 */
public class SemiJoinMapper extends Mapper<Object, Text, Text, CombineValues> {
	
	private CombineValues combineValues = new CombineValues();   
	private HashSet<String> joinKeySet = new HashSet<String>();
    private Text flag = new Text();   
    private Text joinKey = new Text();   
    private Text secondPart = new Text();   
    
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        BufferedReader in = null;
        try {
            // 从当前作业中获取要缓存的文件
        	URI[] uri = DistributedCache.getCacheFiles(context
                    .getConfiguration());
            String line = null;
            FileSystem fs = FileSystem.get(URI.create(Constants.HDFS_ROOT) , context.getConfiguration());
            FSDataInputStream fsin = fs.open(new Path(uri[0].getPath()));
            BufferedReader br=new BufferedReader(new InputStreamReader(fsin));
             while ((line = br.readLine()) != null) {            	 
                 joinKeySet.add(line.split("\t")[0]);
             }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
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
    		//过滤掉不需要参加join的记录   
            if(joinKeySet.contains(vertex.getVertexID())){
            	flag.set("0");   
                joinKey.set(vertex.getVertexID());   
                secondPart.set(vertex.toString());   
                combineValues.setFlag(flag);   
                combineValues.setJoinKey(joinKey);   
                combineValues.setSecondPart(secondPart);   
                context.write(combineValues.getJoinKey(), combineValues);
            }else{
            	return;
            }
        }   
    } 
    
}
