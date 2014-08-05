Map-Reduce-Data-Pivot
=====================

This is a small Map Reduce project to create a general purpose process for pivoting data sets for analysis.

The idea would be familiar to anyone who has used Pivot tables in spreadsheet applications:

Take a set of data that looks like this:

		Store,Product,Number  
		Store-1, Hats, 4  
		Store-1, Shoes, 2  
		Store-1, Gloves, 1  
		Store-2, Hats, 0  
		Store-2, Shoes, 9  
		Store-2, Gloves, 4  
		Store-3, Hats, 2  
		Store-3, Shoes, 0  
		Store-3, Gloves, 3  
		Store-3, Umbrellas, 1  

and turn it into a set that looks like this :  

		Store		Gloves	Hats	Shoes Umbrellas  
		Store-1		1.0,	4.0,	2.0,	0  
		Store-2		4.0,	0.0,	9.0,	0  
		Store-3		3.0,	2.0,	0.0,	1.0  


and be able to that for millions or billions of records and pivot across a very large numbers of columns.  


Usage
-----

hadoop jar {JAR FILE} {AGGREGATION: S=Sum, A=Average, C=Count} {ROW INDEX} {COL INDEX} {DATA INDEX} {INPUT FOLDER} {OUTPUT FOLDER}  

For example  

hadoop jar ~/mybin/MapReduceDataPivot.jar s 1 0 2 testdata testoutput  


Features & Limitations
----------------------

Currently only processes CSV Files  
-- Does not handle Strings containing commas [TODO]  
-- Does not handle missing fields in a row [TODO]  
-- Will not handle Number Format Exceptions gracefully. [TODO]  

Outputs CSV Files  
-- No column headers  
-- Row names followed by a tab and then CSV data  
  
Process expects all of the columns to end up in a single HDFS file following first round of processing.   
For extremely large data sets this will likely break. [TODO]  




