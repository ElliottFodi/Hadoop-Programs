import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
*This program demonstrates a function in the reducer.
*The mapper is passed a record consisting of one number, parses it and sends it to the reducer
*The reducer sums all of the values sent from the mapper and calls average. The reducer emits the average of the values.
*
*Jars required to compile and run
*hadoop-common-2.5.0.jar
*hadoop-mapreduce-client-core-2.5.0.jar
*commons-cli-1.2.jar
*
*run command: bin/Hadoop jar average.jar average input output
*/




public class average extends Configured implements Tool {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

	    public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {

	    	String number = value.toString();
	    	int num = Integer.parseInt(number);
	        context.write(new Text("number"), new IntWritable(num));
	      
	    }
	  }

	  public static class IntSumReducer extends Reducer<Object,IntWritable,Text,IntWritable> {
		  
		  private IntWritable average = new IntWritable();
	    public void reduce(Object key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	
	    	int sum = 0;
	    	int counter = 0;
	    	System.out.println("reduce");
	    	for(IntWritable val: values){
	    		sum += Integer.parseInt(val.toString());
	    		counter++;
	    	}
	    	 
	    	double avg = average(sum, counter);
	    	average.set((int) avg); 
	    	context.write(new Text("number"), average);
	    	 
	    }
	    
	    public double average(int passed_total, int passed_number){
	    	double average = 0;
	    	average = passed_total/(passed_number * 1.0);
	    	return average;
	    }
	  }

	  public static void main(String[] args) throws Exception{
		  int res = ToolRunner.run(new Configuration(), new average(), args);
		  System.exit(res);
	  }
	  
	  public int run(String[] args) throws Exception {
	    Configuration conf = getConf();	    
	    
	    //Job.getInstance provides the config and the job name
	    Job job = Job.getInstance(conf, "number average");
		
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(average.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);

	    //set the data type for the key in the output
	    job.setOutputKeyClass(Text.class);

	    //set the data type for the value in the output
	    job.setOutputValueClass(IntWritable.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	  }

}
