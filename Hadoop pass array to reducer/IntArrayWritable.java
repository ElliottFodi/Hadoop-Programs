import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/*
 *This class extends the ArrayWritable class and defines the type of values the array will be storing 
 *This is used when passing the array from the mapper to the reducer 
 */

public class IntArrayWritable extends ArrayWritable{
	public IntArrayWritable(){
		super(IntWritable.class);
	}
}
