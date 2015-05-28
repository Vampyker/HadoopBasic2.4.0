
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StockDateSumReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, DoubleWritable> {

  public void reduce(Text key, Iterator values,
      OutputCollector output, Reporter reporter) throws IOException {

	try
	{
		double sum = 0;
    		while (values.hasNext()) {
    			DoubleWritable value = (DoubleWritable) values.next();
      			sum += value.get(); // process value
    		}

    		output.collect(key, new DoubleWritable(sum));
	}
	catch(Exception ex)
	{
		System.out.println("Error found:" + ex.getStackTrace()[0]);
	}
  }
}
