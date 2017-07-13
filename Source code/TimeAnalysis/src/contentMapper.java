import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class contentMapper extends Mapper<Object, Text, Text, IntWritable> { 
private Text data = new Text();
private IntWritable numInstances = new IntWritable(1);
String tweetDate;

    public void map(Object key, Text value, Context context) throws IOException, 		InterruptedException {
        	
	String[] inputLines = value.toString().split(";");
	try{	
			//Formating a date to be able to group the tweets by date
	    SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");
        Date tweetsdate1 = new Date(Long.parseLong(inputLines[0]));
		tweetDate = dateFormat.format(tweetsdate1);
        //1 get date
		//2 transform to right format
		//3 make date as string and output as key
		data.set(tweetDate);
		context.write(data,numInstances);
	}
	catch(Exception e){
		System.out.println("Error: " + e);
	}
	
        }
    }


