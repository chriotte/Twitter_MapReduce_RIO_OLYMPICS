import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class contentReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    private LongWritable result = new LongWritable();
    private long numLines = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {
        long sum = (long) 0.0;
        for (IntWritable value : values) {
        	numLines++;
        	sum+=value.get();

        }
        sum = sum / numLines;

        result.set(sum);

	context.write(key, result);

     

    }

}