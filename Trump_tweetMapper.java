import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.FileOutputFormat;
import java.io.*;
import java.util.*;
import org.json.simple.*;
import org.json.simple.parser.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;



public class Trump_tweetMapper extends
        //Mapper<LongWritable,Text,NullWritable,Text>{
        Mapper<LongWritable,Text,Text,Text>{
        JSONParser parser = null;
	    Map<String,String>dictionary  = null;
            
        @Override
    	protected void setup(Context context)throws IOException,InterruptedException
    	{
    		parser = new JSONParser();
    		dictionary = new HashMap<String,String>();
    		
    		
    		
    			URI[] cacheFiles = context.getCacheFiles();
    		
    			if (cacheFiles != null && cacheFiles.length > 0)
    			  {
    			    try
    			    {   
    			    	String line ="";
    			        FileSystem fs = FileSystem.get(context.getConfiguration());
    			        Path path = new Path(cacheFiles[0].toString());
    			        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
    			    
    			        while((line = reader.readLine())!=null)
    			        {
    			        	String []tokens = line.split("\t");
    			        	dictionary.put(tokens[0], tokens[1]);
    			        	 
    			        }
    			
    			    }catch(Exception e)
    			    {
    			    System.out.println("Unable to read the cached filed");
    			    System.exit(1);
    			    }
    			  }
    			
    	}
        

        @Override
        public void map(LongWritable key, Text values, Context context)
            throws IOException, InterruptedException{
                
                
                long sent_value = 0;
                
                try{
                    JSONParser parser = new JSONParser();
                    Object o = parser.parse(values.toString());
                    JSONObject jsonObj = (JSONObject) o;
                    String tweet_id=null;
			        String tweet= null;
			        String location = null;
                    
                    if(null != jsonObj && StringUtils.isNotBlank(String.valueOf(jsonObj))){
                        if(jsonObj.get("tweet_id") !=null &&  jsonObj.get("tweet")!=null && StringUtils.isNotBlank(String.valueOf(jsonObj.get("tweet_id"))) && StringUtils.isNotBlank(String.valueOf(jsonObj.get("tweet")))){
                            tweet_id = String.valueOf(jsonObj.get("tweet_id")).trim();
                            
                            if (StringUtils.isNotBlank(String.valueOf(jsonObj.get("user_location")))){
                                location = String.valueOf(jsonObj.get("user_location")).trim();
                                location = location.replaceAll("[^a-zA-Z\\s]", " ").toLowerCase();
                            }
                            else{
                                location = "Location Missing";
                            }
                            tweet = String.valueOf(jsonObj.get("tweet")).trim();
                            tweet = tweet.replaceAll("(http[^ ]*)|(www\\.[^ ]*)", "").replaceAll("[^a-zA-Z\\s]", "").replaceAll("\\s+", " ").toLowerCase().trim();
                            //tweet = Trump_tweetMapper.removeAllDigit(tweet.replaceAll("\\p{Punct}", " ").toLowerCase());
                            tweet = tweet.replaceAll("[^\\p{L}\\p{M}\\p{N}\\p{P}\\p{Z}\\p{Cf}\\p{Cs}\\s]"," "); // Emoji and special character 
                            tweet = stopWordsRemover.remove(tweet);
                            String []words = tweet.split(" ");

                            for (String t: words){
                                if(dictionary.containsKey(t)){
                                    sent_value +=Long.parseLong(dictionary.get(t));
                            }
                            }
                     
                    if(StringUtils.isNotBlank(tweet_id) && StringUtils.isNotBlank(tweet)){
                        //System.out.println(tweet_id);System.out.println(tweet); System.out.println(location);System.out.println(sent_value);
                        //context.write(NullWritable.get(), new Text(tweet_id+"\t"+tweet+"\t"+sent_value));
                        context.write(new Text (tweet_id), new Text(tweet+"\t"+location+"\t"+sent_value));
                    } 
                }}}
                catch (ParseException e) {
                    e.printStackTrace();	
                }
        }
        
	    public static String removeAllDigit(String str){
	        
	        char[] charArray = str.toCharArray();
	        String result ="";
	        
	        for (int i=0; i< charArray.length;i++){
	            if (!Character.isDigit(charArray[i])){
	                result = result + charArray[i];
	            }
	        }
	        return result;
	    }
	   
}