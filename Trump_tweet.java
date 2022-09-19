import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import java.net.URI;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Trump_tweet {
public static void main(String[] args) throws Exception {
    if (args.length != 2) {
     System.err.println("Usage: trump_mapper <input path> <output path>");
     System.exit(-1);
    }

    Job job = Job.getInstance();
    job.setJarByClass(Trump_tweet.class);
    job.setJobName("Trump Tweet");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(Trump_tweetMapper.class);
    job.setReducerClass(Trump_tweetReducer.class);
    

	//job.setMapOutputKeyClass(NullWritable.class);
	//job.setOutputValueClass(Text.class);
	//job.setOutputKeyClass(NullWritable.class);
	//job.setOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class); //NEw
    job.setOutputValueClass(Text.class); //New
    job.setNumReduceTasks(1);
    
    //job.setOutputKeyClass(TextInputFormat.class);
    //job.setOutputValueClass(TextOutputFormat.class);
    
	try{

	//adding file to distributed cache	
	job.addCacheFile(new URI("/user/kpm8481/Lab5/AFINN-111.txt"));
	}catch(Exception e)
	{
		System.out.println("file not added");
		System.exit(1);
	}
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}