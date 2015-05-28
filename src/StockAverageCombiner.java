import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class StockAverageCombiner extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, DoubleArrayWritable> {
	
	private final int DaySections = 7;
  
	public void reduce(Text key, Iterator values,
      		OutputCollector output, Reporter reporter) throws IOException {
  
		System.out.println("Combiner Started");
		try
		{
			double[] dayarray = new double[DaySections]; // non-writable double array for easier math
			DoubleWritable[] dayArrayWritable = new DoubleWritable[DaySections]; // writable double[] for result
			
			// initialize to 0
			for (DoubleWritable dw : dayArrayWritable)
				dw = new DoubleWritable(0.0);
	    
			// get stock abreviation
			String[] keyparts = key.toString().split("-");
			String keyStock = keyparts[0];
			String keyDay = keyparts[1];
	    
			while (values.hasNext()) 
			{
				DoubleArrayWritable daw_val = (DoubleArrayWritable)values.next(); // input Double Arrary object
	    	
				Writable[] wa = daw_val.get();
		
				int[] dayDivisorArray = new int[] {1,5,10,30,50,100,150};
	    	
				for (int i = 0; i < DaySections; i++)
				{ 
					boolean UseDay = false; // will the day be calculated into the average
					double divisor = 1;		// divisor of the average
		    	
					UseDay = (Integer.parseInt(keyDay) <= dayDivisorArray[i]);
					divisor = dayDivisorArray[i];
	
					if(!UseDay)
						continue;
		    	
					dayarray[i] += (Double.parseDouble(wa[i].toString())/divisor);
		    	
					double daySum = 0.0; // sum of the day-bucket's average parts
					
					if(dayArrayWritable.length < i)
						daySum += dayArrayWritable[i].get();
		    	
					daySum += Double.parseDouble(wa[i].toString());
					dayArrayWritable[i] = new DoubleWritable(daySum);
				}
			}
	    
			for(int i=0; i < DaySections; i++)
				dayArrayWritable[i] = new DoubleWritable (dayarray[i]);
	    
			// DoubleWritable[] -> DoubleArrayWritable
			DoubleArrayWritable daw = new DoubleArrayWritable(dayArrayWritable); // output double array
	
			//unused heading
			//String[] HeadingArray = new String[] {"Stock Symbol","1-Day","5-Day","10-Day","30-Day","50-Day","100-Day"};
	    
			output.collect(new Text(keyStock), daw);
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
  	}
}
