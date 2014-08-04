
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class TableReducer extends Reducer<Text, Text, Text, Text> {
  
	@Override
	public void reduce(
			Text key, 
			Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
		
 		Configuration conf = context.getConfiguration();
 		String pivotMode = conf.get("pivotMode");
 		String defaultValue = conf.get("defaultValue");
 		
 		LinkedHashMap<String, String> myMap = getHashMap(conf);
 		
		Iterator<Text> it=values.iterator();
		
		while (it.hasNext()) {	
			String toProcess = it.next().toString();

			String[] myValues = toProcess.split("\\|\\|");
			for(int i=0; i<myValues.length; i++) {

				String[] keyVal = myValues[i].split("===");

				if(keyVal.length>1) {
					addEntryToHashMap(myMap, keyVal[0], keyVal[1]);
				}
			}
		}
		
		context.write(
				key, 
				new Text(
						getStringRepresentation(myMap, pivotMode, defaultValue)
				) 
		);
	}
 
 	
 	protected LinkedHashMap<String, String> getHashMap(
 			Configuration conf
 		) throws IOException, InterruptedException  {
 		
	 		String columnFolder = conf.get("colNameFolder");
	 		LinkedHashMap<String, String> myMap = new LinkedHashMap<String, String>();
	 		FileSystem hdfs =FileSystem.get(conf);
	        Path  columnDataFilePath = new Path(columnFolder + "/" + "part-r-00000");
	        BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(columnDataFilePath)));   
	        String str = null;
	        while ((str = bfr.readLine())!= null) {
	        	myMap.put(str.trim(), "");
	        }
	        
	 		return myMap;
 	}
 	
 	protected LinkedHashMap<String, String> clearHashMap(
 			LinkedHashMap<String, String> myMap
		) {
 		
 		for (Map.Entry<String, String> entry : myMap.entrySet()) {
 		    String key = entry.getKey();
 		    myMap.put(key, "");
 		}
 		return myMap;
 	}

 	protected void addEntryToHashMap(
 			LinkedHashMap<String, String> myMap, 
 			String myKey, 
 			String myValue
		) {

 		String curentEntry  = myMap.get(myKey);
 		if(curentEntry == "") {
 			myMap.put(myKey, myValue);
 		} else {
 			curentEntry = curentEntry + "||" + myValue;
 			myMap.put(myKey, curentEntry);
 		}
 	}
 	
 	protected String getStringRepresentation(
 			LinkedHashMap<String, String> myMap, 
 			String pivotMode, 
 			String defaultValue
		) {
 		
 		String myString = "";
 		boolean hasData = false;
 		for (Map.Entry<String, String> entry : myMap.entrySet()) {
 		    String value = convertDataToValue( entry.getValue(), pivotMode, defaultValue);
 		    if(hasData) {
 		    	myString = myString + ",";
 		    }
 		    myString = myString + value;
 		    hasData = true;
 		}
 		return myString;
 	}
 	
 	protected String convertDataToValue(
 			String rawDataValues, 
 			String pivotMode, 
 			String defaultValue
		) {
 		
 		String result = "";
 		
 		if(rawDataValues.equals("")) { // NO DATA
 			return defaultValue;
 		}
 		
 		String[] theValues = rawDataValues.split("\\|\\|");
 		
 		if( pivotMode.equals("c") ) { // JUST COUNT THE NUMBER OF ENTRIES
 			result = Integer.toString(theValues.length) ;
 			
 		} else if ( pivotMode.equals("a") ) { // AVERAGE THEM
 			float sum = 0;
 			for(int i=0; i<theValues.length; i++) {
 				sum = sum + Float.parseFloat(theValues[i]);
 			}
 			result = Float.toString(sum / theValues.length);
 			
 		} else if ( pivotMode.equals("s") ) { // SUM THEM
 			float sum = 0;
 			for(int i=0; i<theValues.length; i++) {
 				sum = sum + Float.parseFloat(theValues[i]);
 			}
 			result = Float.toString( sum );
 		} 
 		
 		return result;
 	}
 	
	
}