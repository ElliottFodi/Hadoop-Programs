import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
 *This program creates a writable object that can be passed from the mapper to the reducer. 
 *The writable object is composed of writable primitives.
 *
 *The mapper accepts a line consisting of a name and a number and stores the name and number in the object.
 *The mapper then generates data to place in an array and a map which are then also placed in the object.
 *The generated data is the same for ever mapper because this is a proof of concept and the values being passed don't matter. 
 *
 *The reducer accepts these values and performs the following operations
 *concats the names together
 *sums the numbers 
 *sums the values of the array 
 *concats the values of the map
 *The redcer saves each of these values to another object and emits this object
 *The object that the reducer emits contains a custom toString method which is called when writing the object to an HDFS file
 *
 *Jars required to compile and run
 *hadoop-common-2.5.0.jar
 *hadoop-mapreduce-client-core-2.5.0.jar
 *commons-cli-1.2.jar
 *
 *run command: bin/Hadoop jar customobject.jar customobject input output
 */


public class customobject extends Configured implements Tool {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, teamObject>{

	    public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException {


	    	String line = value.toString();
	    	String[] words = line.split(" ");
			
			//set name an number from record passed
	    	Text name = new Text(words[0]);
	    	IntWritable number = new IntWritable(Integer.parseInt(words[1]));
	    	
			//generate values for the array
	    	IntWritable[] intarray = new IntWritable[4];
	    	for(int i = 0; i < 4; i++ ){
	    		intarray[i] = new IntWritable(i*2);
	    	}
	    	
			//place intWritable array in a ArrayWritable, array writables are used because they can be serialized 
	    	ArrayWritable ar = new ArrayWritable(IntWritable.class);
	    	ar.set(intarray);
	    	
			//generate values for the map
	    	MapWritable map = new MapWritable();
	    	for(int i = 0; i < 4; i++){	    		
	    		map.put(new IntWritable(i), new Text("league" + i));
	    	}
	    	
			//place the map in the object
	    	teamObject obj = new teamObject(name, number, ar, map);
	    	Text id = new Text("test");
	    	
	    	context.write(id, obj);
	      
	    }
	  }

	  public static class IntSumReducer extends Reducer<Text, teamObject, Text, outputObject> {
		  
		  
		  private IntWritable total = new IntWritable();
		  private Text finalName = new Text();
		  private IntWritable arrayTotal = new IntWritable();
		  private Text mapText = new Text();

	    public void reduce(Text key, Iterable<teamObject> values, Context context) throws IOException, InterruptedException {
	    	
	    	int sum = 0;
	    	int arraySum = 0;
	    	String names = "";
	    	String mapConcat = "";
	    	
	    	//iterate over all the objects
	    	for(teamObject val : values){
			
				//concat the names together
	    		names = names + ":" + val.name;
				
				//sum the numbers
	    		sum = sum + Integer.parseInt(val.number.toString());
	    		
				//sum each value in the third index of the array
	    		IntWritable[] intarray = (IntWritable[]) val.array.toArray();
	    		arraySum += intarray[3].get();
	    		
				//concat all the values for the key "3"
	    		MapWritable map = val.map;
	    		mapConcat += map.get(new IntWritable(3));
	    		
	    	}
	    	
			//set the values obtained to a global variable in case they would be used in a further calculation			
	    	total.set(sum); 
	    	finalName.set(names);
	    	arrayTotal.set(arraySum);
	    	mapText.set(mapConcat);
	    	
			//set the new object with the values obtained
	    	outputObject oobj = new outputObject(finalName, total, mapText, arrayTotal);
	    	Text out = new Text("finished");
	    	context.write(out, oobj);
	    		    	 
	    }

	  }

	  public static void main(String[] args) throws Exception{
		  int res = ToolRunner.run(new Configuration(), new customobject(), args);
		  System.exit(res);
	  }
	  
	  public int run(String[] args) throws Exception {
	    Configuration conf = getConf();	    
	    
	    //Job.getInstance provides the config and the job name
	    Job job = Job.getInstance(conf, "number average");
		
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(customobject.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(TokenizerMapper.class);
	    
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    //set the data type for the mapper's key
	    job.setMapOutputKeyClass(Text.class);
	    
	    //set the data type for the mapper's value
        job.setMapOutputValueClass(teamObject.class);        

	    //set the data type for the key in the output of reducer
	    job.setOutputKeyClass(Text.class);

	    //set the data type for the value in the output of reducer
	    job.setOutputValueClass(outputObject.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	  }
	  
	  //Object emitted from the mapper to the reducer 
	  public static class teamObject implements Writable{
		  
		  private Text name;
		  private IntWritable number;
		  private ArrayWritable array;
		  private MapWritable map;
		  
		//need to initialize variables, this is called by hadoop and must be provided along 
		//with the constructor that accepts variables
		public teamObject(){
			name = new Text();
			number = new IntWritable();
			array = new ArrayWritable(IntWritable.class, new IntWritable[4]);
			map = new MapWritable();
		}
		  
		// constructor with passed values
		public teamObject(Text passed_name, IntWritable passed_number, ArrayWritable passed_ArrayWritable, MapWritable passed_map ){
			name = passed_name;
			number = passed_number;
			array = passed_ArrayWritable;
			map = passed_map;
			
		}

		//called when the object is read by the reducer, this is used when de serializing the object
		@Override
		public void readFields(DataInput in) throws IOException {
			name.readFields(in);
			number.readFields(in);
			array.readFields(in);
			map.readFields(in);
		}

		//called when the object is written by the mapper, this is used when serializing the object
		@Override
		public void write(DataOutput out) throws IOException {
			name.write(out);
			number.write(out);
			array.write(out);
			map.write(out);
		}
		
		//custom toString, used if the object is written directly to HDFS, this comes in handy when a reducer is not called 
		public String toString(){
			String print = name.toString() + " " + number.toString();
			return print;
		}
		  
	  }
	  
	  
	  public static class outputObject implements Writable{
		  
		  private Text name;
		  private IntWritable number;
		  private Text mapConcat;
		  private IntWritable arrayTotal;
		  
		  
		//need to initialize variables, this is called by hadoop and must be provided along 
		//with the constructor that accepts variables
		public outputObject(){
			name = new Text();
			number = new IntWritable();
			mapConcat = new Text();
			arrayTotal = new IntWritable();
		}
		  
		// constructor with passed values
		public outputObject(Text passed_name, IntWritable passed_number, 
				Text passed_mapConcat, IntWritable passed_arrayTotal ){
			name = passed_name;
			number = passed_number;
			mapConcat = passed_mapConcat;
			arrayTotal = passed_arrayTotal;
			
		}

		//called when the object is read, this is used when de serializing the object
		@Override
		public void readFields(DataInput in) throws IOException {
			name.readFields(in);
			number.readFields(in);
			mapConcat.readFields(in);
			arrayTotal.readFields(in);
		}

		//called when the object is written, this is used when serializing the object
		@Override
		public void write(DataOutput out) throws IOException {
			name.write(out);
			number.write(out);
			mapConcat.write(out);
			arrayTotal.write(out);
		}
		
		//custom toString, used when the object is written HDFS, this is how the output will look in the results file in HDFS 
		public String toString(){
			String print = name.toString() + " " + number.toString() + " " + 
								arrayTotal.toString() + " " + mapConcat.toString();
			return print;
		}
		  
	  }

}
