import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;


public class DoubleArrayWritable extends ArrayWritable {
        
        public DoubleArrayWritable() {
                super(DoubleWritable.class);
        }
        
        public DoubleArrayWritable(DoubleWritable[] values) {
            super(DoubleWritable.class, values);
        }
        
        public String toString() {
	        StringBuilder sb = new StringBuilder();
	        
	        //for (String s : super.get().toStrings())
	        for (String s : super.toStrings())
	        {
	            sb.append(s).append(" ");
	        }
	        return sb.toString();
        
    	}
        
        

}