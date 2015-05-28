import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;

public class StockDateSumMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	
  private Text word = new Text();
  private String mapTaskId;
  private String inputFile;
  private int noRecords = 0;
  
  public void configure(JobConf job) {
    mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
    inputFile = job.get(JobContext.MAP_INPUT_FILE);
  }
  
  public void map(LongWritable key, Text value,
		  OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

	String line = value.toString();
    String StockSymbol = "";
    
    try
    {
    	// Get Stock Name
    	/*
	    String[] StockPath = inputFile.split("/");
	    String StockFileName = StockPath[StockPath.length - 1];
	    String[] StockFileNameParts = StockFileName.split("\\.");
	    StockSymbol = StockFileNameParts[0];
	    */
	    
	    String[] LineParts = line.split(",");
	    //Date,Open,High,Low,Close,Volume,Adj Close
	    //LineParts[0] - Date
	    //LineParts[1] - Open
	    //LineParts[2] - High
	    //LineParts[3] - Low
	    //LineParts[4] - Close
	    //LineParts[5] - Volume
	    //LineParts[6] - Adj Close
    
   
    	// date is our key
    	word.set(LineParts[0]);
    
    	// skip header
    	if(LineParts[4].equals("Close"))
    		return;
    
    	// date, volume
    	output.collect(word,new DoubleWritable(Double.parseDouble(LineParts[5])));
    }
    catch(Exception ex)
    {	
    	ex.printStackTrace();
    }
  }
}

