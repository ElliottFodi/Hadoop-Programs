import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class wordcount extends Configured implements Tool {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	    	JSONParser par = new JSONParser();
	    	JSONObject obj = new JSONObject();
	    	try {
				obj = (JSONObject) par.parse(value.toString());
			} catch (ParseException e) {
				System.out.println("JSON parse error");
				e.printStackTrace();
			}
	    	
	    	long id = (long) obj.get("id");
	    	String ID = "" + id;
	    	String summoner = (String) obj.get("name");

	        context.write(new Text(summoner), new Text(ID));
	      
	    }
	  }

	  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {


	    public void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
	    	
	      context.write(key, values);
	    }
	  }

	  public static void main(String[] args) throws Exception{
		  int res = ToolRunner.run(new Configuration(), new wordcount(), args);
		  System.exit(res);
	  }
	  
	  public int run(String[] args) throws Exception {
	    Configuration conf = getConf();
	    //GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
	    //String[] remainingArgs = optionsParser.getRemainingArgs();
	    
	    
	    //Job.getInstance provides the config and the job name
	    Job job = Job.getInstance(conf, "word count");
	    // set jar by finding where the given class name came from 
	    job.setJarByClass(wordcount.class);

	    //set the mapper, reducer, and the combiner 
	    //with the above classes we defined
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);

	    //set the data type for the key in the output
	    job.setOutputKeyClass(Text.class);

	    //set the data type for the value in the output
	    job.setOutputValueClass(Text.class);

	    //provide the location of the input and output directory 
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	  }

}
