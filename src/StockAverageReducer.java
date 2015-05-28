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

public class StockAverageReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, DoubleArrayWritable> {
	
	private final int DaySections = 7;
	
	public void reduce(Text key, Iterator values,
      		OutputCollector output, Reporter reporter) throws IOException {
  
		System.out.println("Reducer Started");
		try {
			double[] dayarray = new double[DaySections];
			DoubleWritable[] dayArrayWritable = new DoubleWritable[DaySections];
			
			for (DoubleWritable dw : dayArrayWritable)
				dw = new DoubleWritable(0.0);
	    	
	        	while (values.hasNext())  {
        			DoubleArrayWritable daw_val = (DoubleArrayWritable)values.next();	
        			Writable[] wa = daw_val.get();

				for (int i = 0; i < DaySections; i++) {
		    			dayarray[i] += Double.parseDouble(wa[i].toString());
		    			double daySum = 0.0; 
		    			if(dayArrayWritable.length < i)
		    				daySum += dayArrayWritable[i].get();
		    	
		    			daySum += Double.parseDouble(wa[i].toString());
		    			dayArrayWritable[i] = new DoubleWritable(daySum);
		    			//dayArrayWritable[i].set(daySum);
		    		}
        		}
    
        		for(int i=0; i < DaySections; i++)
        			dayArrayWritable[i] = new DoubleWritable (dayarray[i]);
    
        		DoubleArrayWritable daw = new DoubleArrayWritable(dayArrayWritable);

		        //unused heading
		        //String[] HeadingArray = new String[] {"Stock Symbol","1-Day","5-Day","10-Day","30-Day","50-Day","100-Day"};

		        output.collect(key, daw);
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
  	}
}
