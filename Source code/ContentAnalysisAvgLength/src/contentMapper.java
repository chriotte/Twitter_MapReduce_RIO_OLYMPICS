import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

public class contentMapper extends Mapper<Object, Text, Text, IntWritable> { 
	


private Text data = new Text();
private IntWritable numInstances = new IntWritable();
private int tweetLng;

    public void map(Object key, Text value, Context context) throws IOException, 		InterruptedException {
        	
	String[] inputLines = value.toString().split(";");
	
	try{
		
	if (inputLines.length >= 3) { //To check if there is content in the tweet
		tweetLng = inputLines[2].length(); //count length
		if (tweetLng <= 140){
		data.set("Average: "); //Average is the key for the reducer
		numInstances.set(tweetLng);
		context.write(data,numInstances);
		}

	}
	
	}
	catch(Exception e){
		e.printStackTrace();
	}
	
        }
    }

