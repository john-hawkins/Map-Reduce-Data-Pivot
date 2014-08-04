
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ColumnMapper extends Mapper<Object, Text, Text, Text> {
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

 		String[] splitLine = value.toString().split(",");
 		
 		if(splitLine.length < colIndex) {	
 			// There is a conflict between the data row and the 
 			// CSV columns specified in the set-up
 		} else {
 			
 			String colKey = splitLine[colIndex];

 			keyStr.set(colKey);
 			valStr.set(colKey);
 	 		context.write(keyStr, valStr);		
 		}
	}
 	
}