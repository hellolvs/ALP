package lvs.util;


public class Vertex {

	private String vertexID;
	private String vertexvalue;
	private String vertexlabel;
	private String edges = null;

	/**
	 *数据格式为"vertexID:vertexValue@vertexlabel	edges"(初始数据可能没有标签)，以数据"1:0@1	2:0 5:0 6:0 7:0"为例
	 *首先用"\t"对数据进行切割得到IDvalueandlabel="1:0@1"，edges="2:0 5:0 6:0 7:0"
	 *再用"@"进行分割IDvalueandlabel得到IDandvalue=1:0,vertexlabel=1
	 *最后用":"进行分割IDandvalue得到vertexID=1,vertexvalue=0
	 */
	public void fromString(String text) {
		String[] kv = text.split("\t");
		String[] IDvalueandlabel = kv[0].split("@");
		String[] IDandvalue = IDvalueandlabel[0].split(":");			
		if(IDvalueandlabel.length == 2){			
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			vertexlabel = IDvalueandlabel[1];
		} else {
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			vertexlabel = Integer.toString(0);
		}
		if(kv.length == 2){
			edges = kv[1];
		}
	}
    
	public void fromString(String text, int partitionNum) {
		String[] kv = text.split("\t");
		String[] IDvalueandlabel = kv[0].split("@");
		String[] IDandvalue = IDvalueandlabel[0].split(":");
		if(IDvalueandlabel.length == 2){			
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			vertexlabel = IDvalueandlabel[1];
		} else {
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			//Integer randomlabel = (int)(1+Math.random()*partitionNum);
			//vertexlabel = randomlabel.toString();
			HashPartitioner<String> hashPartitioner = new HashPartitioner<String>(partitionNum);
			Integer partitionID = hashPartitioner.getPartitionID(vertexID);
			vertexlabel = partitionID.toString();
		}//对无标签的初始数据生成随机标签（Hash划分）
		if(kv.length == 2){
			edges = kv[1];
		}
	}
	
	public void fromString(String text, int partitionNum, int degree) {
		String[] kv = text.split("\t");
		String[] IDvalueandlabel = kv[0].split("@");
		String[] IDandvalue = IDvalueandlabel[0].split(":");
		if(kv.length == 2){
			edges = kv[1];
		}
		if(IDvalueandlabel.length == 2){			
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			vertexlabel = IDvalueandlabel[1];
		} else if(edges != null && edges.split(" ").length >= degree){
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			//Integer randomlabel = (int)(1+Math.random()*partitionNum);
			//vertexlabel = randomlabel.toString();
			HashPartitioner<String> hashPartitioner = new HashPartitioner<String>(partitionNum);
			Integer partitionID = hashPartitioner.getPartitionID(vertexID);
			vertexlabel = partitionID.toString();
		} else{
			vertexID = IDandvalue[0];
			vertexvalue = IDandvalue[1];
			vertexlabel = Integer.toString(0);
		}//对高度数顶点进行Hash划分，其余顶点标签置标志位0，表示未划分
	}
	
	public String toString() {
		if (this.edges != null) {
			return vertexID + ":" + vertexvalue + "@" + vertexlabel + "\t" + edges;
					
		} else {
			return vertexID + ":" + vertexvalue + "@" + vertexlabel;
			
		}
	}

	public String[] getedgearary() {
		return this.edges.split(" ");
	}

	public String getIDvalueandLabel() {
		return vertexID + ":" + vertexvalue + "@" + vertexlabel;
	}

	public String getIDandvalue() {
		return vertexID + ":" + vertexvalue;
	}

	public String getVertexID() {
		return vertexID;
	}

	public void setVertexID(String vertexID) {
		this.vertexID = vertexID;
	}

	public String getEdges() {
		return edges;
	}

	public void setEdges(String edges) {
		this.edges = edges;
	}

	public String getVertexlabel() {
		return vertexlabel;
	}

	public void setVertexlabel(String vertexlabel) {
		this.vertexlabel = vertexlabel;
	}
	
	public String getVertexvalue() {
		return vertexvalue;
	}
	
	public void setVertexvalue(String vertexvalue) {
		this.vertexvalue = vertexvalue;
	}

}
