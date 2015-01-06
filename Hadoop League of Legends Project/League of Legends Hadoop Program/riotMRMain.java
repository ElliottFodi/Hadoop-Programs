import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class riotMRMain extends Configured implements Tool {

	 public static void main(String[] args) throws Exception{
		  int res = ToolRunner.run(new Configuration(), new riotMRMain(), args);
		  System.exit(res);
	  }
	  
	  public int run(String[] args) throws Exception {
	    Configuration conf = getConf();   
	    
	    //Job.getInstance provides the config and the job name
	    Job job = Job.getInstance(conf, "riotMRMain");
	    
	    //set the class containg the main
	    job.setJarByClass(riotMRMain.class);

	    //point the mapper to the mapper class and the reducer to the reducer class
	    job.setMapperClass(mapper.class);
	    job.setReducerClass(reducer.class);
	    
	    //set what the mapper outputs since the reducer outputs something different
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(TeamWritable.class);

	    //set what the reducer outputs
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(resultsAggregator.class);
	    
	    //job.setNumReduceTasks(1);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    return job.waitForCompletion(true) ? 0 : 1;
	  }
}
