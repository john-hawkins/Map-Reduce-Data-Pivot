
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class PivotReducer extends Reducer<Text, Text, Text, Text> {

	private StringBuffer totalPubList = new StringBuffer();
  
	@Override
	public void reduce(
			Text key, 
			Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
 		
		Iterator<Text> it=values.iterator();
		boolean hasData = false;
		while (it.hasNext()) {
			if(hasData) {
				totalPubList.append("||");
			}
			totalPubList.append( it.next() );
			hasData = true;
		}
		context.write(key, new Text(totalPubList.toString()) );
		totalPubList = new StringBuffer();
	}
 
}