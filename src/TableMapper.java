
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TableMapper extends Mapper<Object, Text, Text, Text> {
	private Text keyStr = new Text();
	private Text valStr = new Text();
  
 	@Override
	public void map(
			Object key, 
			Text value, 
			Context context
	) throws IOException, InterruptedException {

 		String[] splitLine = value.toString().split("\t");
 		/*
 		 * We are just going to emit the key value pairs from the input
 		 * All work is done in the Reducer
 		 */
 		if(splitLine.length >1) {	
 			keyStr.set(splitLine[0]);
 			valStr.set(splitLine[1]);
 	 		context.write(keyStr, valStr);		
 		}
	}
 	
}