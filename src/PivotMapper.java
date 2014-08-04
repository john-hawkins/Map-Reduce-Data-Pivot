
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PivotMapper extends Mapper<Object, Text, Text, Text> {
	private Text keyStr = new Text();
	private Text valStr = new Text();
  
 	@Override
	public void map(
			Object key, 
			Text value, 
			Context context
	) throws IOException, InterruptedException {
 		
 		Configuration conf = context.getConfiguration();
 		
 		int colIndex = Integer.parseInt(conf.get("colIndex"));
 		int rowIndex = Integer.parseInt(conf.get("rowIndex"));
 		int dataIndex = Integer.parseInt(conf.get("dataIndex"));

 		String[] splitLine = value.toString().split(",");
 		
 		int maxIndex = dataIndex;
 		if (maxIndex < colIndex) {
 			maxIndex = colIndex;
 		}
 		if (maxIndex < rowIndex) {
 			maxIndex = rowIndex;
 		}
 		
 		if(splitLine.length < maxIndex) {	
 			// There is a conflict between the data row and the 
 			// CSV columns specified in the set-up
 		} else {
 			
 			String rowKey = splitLine[rowIndex].trim();
 			String colKey = splitLine[colIndex].trim();
 			String data = splitLine[dataIndex].trim();

 	 		keyStr.set(rowKey);
 	 		valStr.set(colKey + "===" + data);
 	 		context.write(keyStr, valStr);		
 		}
	}
 	
}