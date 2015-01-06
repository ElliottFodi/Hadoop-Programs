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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 *This program passes an array from the mapper to the reducer, in order to pass an array the ArrayWritable 
 *class needs to be extended to specify the type of values stored in the array
 *
 *The mapper accepts a record with a name and four numbers following the name. The record is parsed and the 
 *numbers are placed in the array, the mapper emits the name and the array to the reducer 
 *
 *The reducer accepts the array and adds the numbers as an intWriatble to an array list. 
 *The numbers are then concatenated together in a string.
 *The difference between the out put file and the input file is that their should be no spaces between the numbers 
 *
 *I originally tried to just pass an ArrayWritable to the reducer but this will not work, a solution was not present on 
 *Hadoops web site, so I browsed the forms to, figure out why this would not work. IntArrayWritable extends array writable but 
 *specifies the Array type as ints. More complex examples can be found on line. 
 *
 *Jars required to compile and run
 *hadoop-common-2.5.0.jar
 *hadoop-mapreduce-client-core-2.5.0.jar
 *commons-cli-1.2.jar
 *
 *run command: bin/Hadoop jar hadooparray.jar hadooparray input output
 */


public class hadooparray {

	public static class arrayMapper extends Mapper<Object, Text, Text, IntArrayWritable>{

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	    	String line = value.toString();
	    	String[] elements = line.split(" "); 
	    	
    		IntArrayWritable numbers = new IntArrayWritable();
    		IntWritable nums[] = new IntWritable[4];
    		
    		Text title = new Text(elements[0]);
    		nums[0] = new IntWritable(Integer.parseInt(elements[1]));
    		nums[1] = new IntWritable(Integer.parseInt(elements[2]));
    		nums[2] = new IntWritable(Integer.parseInt(elements[3]));
    		nums[3] = new IntWritable(Integer.parseInt(elements[4]));
    		
    		numbers.set(nums);
    		context.write(title, numbers);
	      
	    }
	  }

	  public static class arrayReducer extends Reducer <Text, IntArrayWritable, Text, Text> {


	    public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

	    	ArrayList<Integer> arraylist = new ArrayList<Integer>();
	    	for(IntArrayWritable val : values){
	    		for(Writable writable : val.get()){
	    			IntWritable intwritable = (IntWritable) writable;
	    			arraylist.add(intwritable.get());
	    		}
	    	}
	    	
	    	String numbers = "";
	    	for(int i = 0; i < arraylist.size(); i++){
	    		numbers = numbers + arraylist.get(i);
	    	}
	      context.write(key, new Text(numbers));
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
		
	    //Job.getInstance provides the config and the job name
	    Job job = Job.getInstance(conf, "hadooparray");
		
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(hadooparray.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(arrayMapper.class);
	    job.setReducerClass(arrayReducer.class);

	    //set the data type for the key in the output
	    job.setOutputKeyClass(Text.class);

	    //set the data type for the value in the output
	    job.setOutputValueClass(IntArrayWritable.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
