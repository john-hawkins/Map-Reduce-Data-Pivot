import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataPivot {
	
	public static void main(String[] args) throws Exception {
        if (args.length < 6) {
        	 printUsage(); 
        	 System.exit(-1);
        }
        char pivotMode = 'c';
        int rowIndex = 0;
        int colIndex = 1;
        int dataIndex = 2;
        String inputFolder = "";
        String outputFolder =  "";
        String defaultValue = "0";
	    try{
	        pivotMode = args[0].charAt(0);
	        colIndex = Integer.parseInt(args[1]);
	        rowIndex = Integer.parseInt(args[2]);
	        dataIndex = Integer.parseInt(args[3]);
	        inputFolder = args[4];
	        outputFolder = args[5];
	        if (args.length == 7) {
	        	defaultValue = args[6];
	        }
        } catch(Exception e) { // Let's assume parsing problems
        	System.out.println("ERROR PARSING ARGUMENTS -- PLEASE CHECK"); 
        	printUsage(); 
        	System.exit(-1);
        }
        Configuration conf = new Configuration();
        
        String jobOneOutputFolder =  "temp_job_one_" + outputFolder;
        String jobTwoOutputFolder =  "temp_job_two_" + outputFolder;
        
        conf.set("pivotMode", Character.toString(pivotMode) );
        conf.set("colIndex", Integer.toString(colIndex) );
        conf.set("rowIndex", Integer.toString(rowIndex) );
        conf.set("dataIndex", Integer.toString(dataIndex) );
        conf.set("defaultValue", defaultValue );
        conf.set("colNameFolder", jobTwoOutputFolder );

        /*
         *  First Job collects all the values for each Row key
         */
        Job jRowBuilder = Job.getInstance(conf);
        
        jRowBuilder.setOutputKeyClass(Text.class);
        jRowBuilder.setOutputValueClass(Text.class);
 
        jRowBuilder.setMapperClass(PivotMapper.class);
        jRowBuilder.setReducerClass(PivotReducer.class); 
 
        jRowBuilder.setInputFormatClass(TextInputFormat.class);
        jRowBuilder.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(jRowBuilder, new Path(inputFolder) );
        FileOutputFormat.setOutputPath(jRowBuilder, new Path(jobOneOutputFolder) );
 
        jRowBuilder.setJarByClass(DataPivot.class);

        ControlledJob jobRowBuilder = new ControlledJob(conf);
        jobRowBuilder.setJob(jRowBuilder);
        
        /*
         *  Second Job Just Builds the List of Column Headers
         *  
         *  This is an implementation of the 'Uniq' Map-Reduce Pattern
         *  with the minor modification that it expects CSV data and 
         *  a column index set in the configuration
         */
        Job jColBuilder = Job.getInstance(conf);
        
        jColBuilder.setOutputKeyClass(Text.class);
        jColBuilder.setOutputValueClass(Text.class);
 
        jColBuilder.setMapperClass(ColumnMapper.class);
        jColBuilder.setReducerClass(ColumnReducer.class); 
 
        jColBuilder.setInputFormatClass(TextInputFormat.class);
        jColBuilder.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(jColBuilder, new Path(inputFolder) );
        FileOutputFormat.setOutputPath(jColBuilder, new Path(jobTwoOutputFolder) );
 
        jColBuilder.setJarByClass(DataPivot.class);

        ControlledJob jobColBuilder = new ControlledJob(conf);
        jobColBuilder.setJob(jColBuilder);
        
        
        /*
         *  Final Job uses the output of the first two to collate the table
         *  
         *  Note: It accesses the results of the second job directly in the HDFS
         *  File system. The output of this job is expected to be a single file
         *  at a specified location.
         */

        Job jTabBuilder = Job.getInstance(conf);
        
        jTabBuilder.setOutputKeyClass(Text.class);
        jTabBuilder.setOutputValueClass(Text.class);
 
        jTabBuilder.setMapperClass(TableMapper.class);
        jTabBuilder.setReducerClass(TableReducer.class); 
 
        jTabBuilder.setInputFormatClass(TextInputFormat.class);
        jTabBuilder.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.setInputPaths(jTabBuilder, new Path(jobOneOutputFolder) );
        FileOutputFormat.setOutputPath(jTabBuilder, new Path(outputFolder) );
 
        jTabBuilder.setJarByClass(DataPivot.class);

        ControlledJob jobTableBuilder = new ControlledJob(conf);
        jobTableBuilder.setJob(jTabBuilder);
        
        /*
         * Put it all together
         * 
         * This doesn't work, the job fails because
         */
        JobControl jbcntrl=new JobControl("jbcntrl");
        
        jbcntrl.addJob(jobRowBuilder); 
        jbcntrl.addJob(jobColBuilder); 
        jobTableBuilder.addDependingJob(jobRowBuilder); 
        jobTableBuilder.addDependingJob(jobColBuilder); 
        jbcntrl.addJob(jobTableBuilder); 
        jbcntrl.run();
        
 	}
 
	 private static void printUsage() {
	     System.out.println("Usage: DataPivot [s|a|c] colsIndex rowsIndex dataIndex input output [default=0]");
	     System.out.println("		First Argument: Pivot Mode ");
	     System.out.println("		                s = Sum Data (Requires Numeric Field)");
	     System.out.println("		                a = Average Data (Requires Numeric Field)");
	     System.out.println("		                c = Count Data (Count Occurences)");
	     System.out.println("		Second Argument: Columns Index = The field index that will become");
	     System.out.println("		                                 the columns of the pivot table ");
	     System.out.println("		Third Argument: Rows Index = The field index that will become");
	     System.out.println("		                                 the rows of the pivot table ");
	     System.out.println("		Fourth Argument: Data Index = The field index that will become");
	     System.out.println("		                                 the data cells of the pivot table ");
	     System.out.println("		Fifth Argument: input = The folder name in HDFS where the input files");
	     System.out.println("		                        can be found. The data must be in CSV format!!!.");
	     System.out.println("		Sixth Argument: output = The folder name in HDFS where the output goes.");
	     System.out.println("		Seventh Argument: default = The value that goes in cells with no data."); 
	 }
}