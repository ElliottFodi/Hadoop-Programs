import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 *This program emits a regular Java object in the reducer, this is done to prove that the reducer does not need to emit writable objects
 *Since the reducer is writing to HDFS, the object emitted does not need to be writable, it just needs a toString method
 *
 *The mapper accepts a number per record and emits that number, the mapper is very simple because the focus is on the reducer
 *Since values are hard coded the mapper expects one file with three numbers, one number per line
 *
 *The reducer accepts these values and inserts them into the java object. These values are hard coded as this is a proof of concept.
 *If this is used in any other setting the array values should not be hard coded
 *The reducer emits the java object 
 *
 *Jars required to compile and run
 *hadoop-common-2.5.0.jar
 *hadoop-mapreduce-client-core-2.5.0.jar
 *commons-cli-1.2.jar
 *
 *run command: bin/Hadoop jar reducerObject.jar reducerObject input output
 */


public class reducerObject extends Configured implements Tool {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	    public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException {
	    	int number = Integer.parseInt(value.toString());
	    	
	      context.write(new Text("out"), new IntWritable(number));
	    }
	  }

	public static class IntSumReducer extends Reducer<Text, IntWritable, String, ArrayList<myObject>> {
		  

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			ArrayList<myObject> al = new ArrayList<myObject>();
			int[] numbers = new int[3];
			int i = 0;
			for(IntWritable val : values ){
				numbers[i] = val.get();
				i++;
			}

			for(int k = 0; k < 3; k++){
				al.add(new myObject(numbers[0], numbers[1], numbers[2]));
			}
			
			context.write("result", al);
						 
		}

	}

	  public static void main(String[] args) throws Exception{
		  int res = ToolRunner.run(new Configuration(), new reducerObject(), args);
		  System.exit(res);
	  }
	  
	  public int run(String[] args) throws Exception {
	    Configuration conf = getConf();	    
	    
	    //Job.getInstance provides the config and the job name
	    Job job = Job.getInstance(conf, "object");
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(reducerObject.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(TokenizerMapper.class);
	    
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    //set the data type for the mapper's key
	    job.setMapOutputKeyClass(Text.class);
	    
	    //set the data type for the mapper's value
        job.setMapOutputValueClass(IntWritable.class);        

	    //set the data type for the key in the output of reducer
	    job.setOutputKeyClass(String.class);

	    //set the data type for the value in the output of reducer
	    job.setOutputValueClass(ArrayList.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	  }
	  
	 
	  //regular Java object emitted by the reducer 
	  public static class myObject{
		  
		  int var1 = 0;
		  int var2 = 0; 
		  int var3 = 0;
		  
		  public myObject(int passed_var1, int passed_var2, int passed_var3){
			  var1 = passed_var1;
			  var2 = passed_var2;
			  var3 = passed_var3;
		  }
		  
		  //toString called when writing the object to HDFS
		  public String toString(){
			  String line = "Var1: " + var1 + ", Var2: " + var2 + ", Var3: " + var3;
			  
			  return line;
		  }
		  
		  
	  }
	 
}
