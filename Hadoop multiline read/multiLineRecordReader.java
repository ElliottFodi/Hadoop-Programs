import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/*
* I used the instructions on 
*http://bigdatacircus.com/2012/08/01/wordcount-with-custom-record-reader-of-textinputformat/ by Shantanu Deo 
*to create this custom record reader, as I could not find any instructions on how to complete this on Hadoop web site
*
*This class extends record reader and implements the required methods 
*these methods are called when reading a record from a file. For this implementation 
*the record will consist of two lines instead of the default one line
*
*Most of the complexity is done in the initialize and nextKeyValue methods
*/


public class multiLineRecordReader extends RecordReader<LongWritable, Text>{
    private final int NLINESTOPROCESS = 2;
    private LineReader in;
    private LongWritable key;
    private Text value = new Text();
    private long start =0;
    private long end =0;
    private long pos =0;
    private int maxLineLength;
 
public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
 
public LongWritable getCurrentKey() throws IOException,InterruptedException {
        return key;
    }
 
public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }
 
public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        }
        else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }
 
/*
 *This method sets the starting point for the reader. In this case the starting point is still the first line of the file 
 *but if several lines needed to be skipped per file, this is where that would be done.
 *This is called every time a new file is being read. For this implementation it is essentially the standard Hadoop implementation
 *as said the initial starting point for the reader is not changed
 */
public void initialize(InputSplit genericSplit, TaskAttemptContext context)throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        final Path file = split.getPath();

        Configuration conf = context.getConfiguration();
        this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
        FileSystem fs = file.getFileSystem(conf);
        
        start = split.getStart();
        end= start + split.getLength();
        
        boolean skipFirstLine = false;
        FSDataInputStream filein = fs.open(split.getPath());
 
        if (start != 0){
            skipFirstLine = true;
            --start;
            filein.seek(start);
        }
        
        in = new LineReader(filein,conf);
        
        if(skipFirstLine){
            start += in.readLine(new Text(),0,(int)Math.min((long)Integer.MAX_VALUE, end - start));
        }
        
        this.pos = start;
    }
	
/*
 *This methods defines how many lines should be read, and sets the key and value to be emitted.
 */ 
 
public boolean nextKeyValue() throws IOException, InterruptedException {
        
        if (key == null) {
            key = new LongWritable();
        }
        
        key.set(pos);
        
        if (value == null) {
            value = new Text();
        }
        
        value.clear();
        final Text endline = new Text("\n");
        int newSize = 0;
        
        for(int i=0; i<NLINESTOPROCESS; i++){
            Text v = new Text();
        
            while (pos < end) {
                //v is the object to store the line in
                //maxLineLength is the number of bytes to store in "v"
                //the last value is the max bytes to consume, the max number of bytes to consume in this call... this is only a hint, it can be over shot
                //get the line
                newSize = in.readLine(v,    maxLineLength,    Math.max((int)Math.min(Integer.MAX_VALUE, end-pos), maxLineLength));
                //append the line to value
                value.append(v.getBytes(),0, v.getLength());
                //append a \n to the line
                value.append(endline.getBytes(),0, endline.getLength());
        
                //newSize is the number of bytes read, if this is zero ... we didnt read anything
                if (newSize == 0) {
                    break;
                }
        
                pos += newSize;
        
                if (newSize < maxLineLength) {
                    break;
                }
            }
        }
        
        //bytes read include line break ... so if this triggers... it is the end of the file
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}
