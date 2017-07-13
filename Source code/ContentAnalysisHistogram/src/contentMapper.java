import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class contentMapper extends Mapper<Object, Text, IntWritable, IntWritable> { 

private IntWritable data = new IntWritable();
private IntWritable numInstances = new IntWritable(1);
private int tweetLng;
    public void map(Object key, Text value, Context context) throws IOException, 		InterruptedException {
       	
	String[] inputLines = value.toString().split(";");
	try{
	if (inputLines.length >= 3) {
		tweetLng = inputLines[2].length(); //count length
		data.set(tweetLng - ((tweetLng - 1) % 5 )+  4);
		context.write(data,numInstances);
	}
	}
	catch(Exception e){
		System.out.println("Error: " + e);
	}
	
        }
    }


