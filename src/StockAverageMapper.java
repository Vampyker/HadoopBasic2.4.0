import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
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

public class StockAverageMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, DoubleArrayWritable> {

  private final int DaySections = 7;
 
  private String mapTaskId;
  private String inputFile;
  
  private static List<KeyDayPair> StockDayOrderList = new ArrayList<KeyDayPair>();
  
  private int GetDayOfKeyInStockDayOrder(String key)
  {
	  int index = IndexOfKeyInStockDayOrder(key);
	  if(index == -1)
	  {
		  return -1;
	  }
	  else
	  {
		  return StockDayOrderList.get(index).Day;
	  }
  }
  
  private int IndexOfKeyInStockDayOrder(String key)
  {
	  for(int i = 0; i < StockDayOrderList.size(); i++)
	  {
		  KeyDayPair currentKeyDayPair = StockDayOrderList.get(i);
		  if(currentKeyDayPair.Key.equals(key))
		  {
			  return i;
		  }
	  }
	  return -1;
  }
  
  private void IncrementStockDayOrderByKey(String key)
  {
	  int index = IndexOfKeyInStockDayOrder(key);
	  if(index == -1)
	  {
		  StockDayOrderList.add(new KeyDayPair(key,1));
	  }
	  else
	  {
		  KeyDayPair currentKeyDayPair = StockDayOrderList.get(index);
		  StockDayOrderList.set(index, new KeyDayPair(key,currentKeyDayPair.Day+1));
	  }
  }
  
  public void configure(JobConf job) {
    mapTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
    inputFile = job.get(JobContext.MAP_INPUT_FILE);
  }
  
  public void map(LongWritable key, Text value,
		  OutputCollector<Text, DoubleArrayWritable> output, Reporter reporter) throws IOException {

	String line = value.toString();
    String StockSymbol = "";

    //ArrayPrimitiveWritable apw = new ArrayPrimitiveWritable();
    
    //double[] dayarray = new double[DaySections];
    
    DoubleWritable[] dayarraywritable = new DoubleWritable[DaySections];
    
    String[] LineParts = line.split(",");
    
    try
    {
        // skip header
        if(LineParts[4].equals("Close"))
        	return;
        
	    String[] StockPath = inputFile.split("/");
	    String StockFileName = StockPath[StockPath.length - 1];
	    String[] StockFileNameParts = StockFileName.split("\\.");
	    
	    IncrementStockDayOrderByKey(StockFileNameParts[0]);
	    int dayOfStock = GetDayOfKeyInStockDayOrder(StockFileNameParts[0]);
	    
	    StockSymbol = StockFileNameParts[0] + "-" + String.format("%05d", dayOfStock);

	    //Date,Open,High,Low,Close,Volume,Adj Close
	    //LineParts[0] - Date
	    //LineParts[1] - Open
	    //LineParts[2] - High
	    //LineParts[3] - Low
	    //LineParts[4] - Close
	    //LineParts[5] - Volume
	    //LineParts[6] - Adj Close
        
	    for(int i = 0; i < DaySections;i++)
	    {
	    	//dayarray[i] = Double.parseDouble(LineParts[4]);
	    	//dayarraywritable[i] = new DoubleWritable(dayarray[i]);
	    	dayarraywritable[i] = new DoubleWritable(Double.parseDouble(LineParts[4]));
	    }

    	//apw.set(dayarray);
    	DoubleArrayWritable daw = new DoubleArrayWritable(dayarraywritable);

    	output.collect(new Text(StockSymbol),daw);
    }
    catch(Exception ex)
    {	
    	ex.printStackTrace();
    }
  }
}

