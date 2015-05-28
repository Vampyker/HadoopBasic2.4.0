import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class StockDateSum {
	public static void main(String[] args) {

		if(args.length < 2)
		{
			System.out.println("You must have at least two arguments.");
			return;
		}
		
		System.out.println("arg1: " + args[0]);
		System.out.println("arg2: " + args[1]);
	  
		JobClient client = new JobClient();
	    	JobConf conf = new JobConf(StockDateSum.class);
	
	    	// specify output types
	    	conf.setOutputKeyClass(Text.class);
	    	conf.setOutputValueClass(DoubleWritable.class);
	
	    	// specify input and output dirs
	    
	    	// 1 FILE!
	    	//FileInputFormat.addInputPath(conf, new Path("hdfs://localhost:9000/input3"));
	    
	    	// ALL FILES!
	    	//FileInputFormat.addInputPath(conf, new Path("hdfs://localhost:9000/input2/hadoop project data"));
	    	
	    	FileInputFormat.addInputPath(conf, new Path(args[0].toString()));
	    	//FileInputFormat.addInputPath(conf, new Path("hdfs://localhost:9000/input"));
	    	FileOutputFormat.setOutputPath(conf, new Path(args[1].toString()));
	    	//FileOutputFormat.setOutputPath(conf, new Path("output"));
	
	    	conf.setMapperClass(StockDateSumMapper.class);
	    	conf.setReducerClass(StockDateSumReducer.class);
		conf.setCombinerClass(StockDateSumReducer.class);
	
	    	client.setConf(conf);
	    	try {
	      		System.out.println("Pre-RunJob - OK (StockDateSum)");
	      		System.out.println("Local Dirs: " + conf.getLocalDirs()[0].toString());
	      		JobClient.runJob(conf);
	      		System.out.println("Post-RunJob - OK");
	    	} 
	    	catch (Exception e) {
	      		e.printStackTrace();
	    	}
  	}
}
