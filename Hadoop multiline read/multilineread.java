import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
*This program reads passes two lines as a record  
*The mapper is passed a record consisting of two lines, of words, it will emit the last word of 
*the second line as the key and the total count of all the words in each line
*
*Jars required to compile and run
*hadoop-common-2.5.0.jar
*hadoop-mapreduce-client-core-2.5.0.jar
*commons-cli-1.2.jar
*
*The reducer will simple emit the key/value pair and perform no calculation on them. Technically the reducer is not needed
*run command: bin/Hadoop jar multilineread.jar multilineread input output
*/


public class multilineread {

	public static class arrayMapper extends Mapper<Object, Text, Text, IntWritable>{

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	    	String line = value.toString();
	    	String[] lines = line.split("\n");
	    	
	    	String[] elementsLine1 = lines[0].split(" "); 
	    	String[] elementsLine2 = lines[1].split(" "); 
	    	IntWritable words = new IntWritable(elementsLine1.length + elementsLine2.length);
	    	
    		Text title = new Text(elementsLine2[elementsLine2.length - 1]);

    		context.write(title, words);
	      
	    }
	  }

	  public static class arrayReducer extends Reducer <Text, IntWritable, Text, Text> {

	    public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {

	      Text words = new Text(values); 
	      context.write(key, words);
		  
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
		
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(multilineread.class);
		
		//set custom reader here
	    job.setInputFormatClass(multiLineFormat.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(arrayMapper.class);
	    job.setReducerClass(arrayReducer.class);

	    //set the data type for the key in the output
	    job.setOutputKeyClass(Text.class);

	    //set the data type for the value in the output
	    job.setOutputValueClass(IntWritable.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}

