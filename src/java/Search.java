import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author sgara
 */

public class Search extends Configured implements Tool {
 private static final Logger LOG = Logger .getLogger( Search.class);
 public static ArrayList<String> inputArgs = new ArrayList<String>();
 public static String input = "";
 public static void main( String[] args) throws  Exception {
    int res  = ToolRunner .run( new Search(), args);
    System .exit(res);
 }
 public int run( String[] args) throws  Exception {
	   
	   for(int i=2;i<args.length;i++) {
	    	  inputArgs.add(args[i]);
	      }
	   
    Job job  = new Job(getConf(), " SearchHits ");
    job.setJarByClass( this .getClass());
    //Input is taken as an array
    String[] newInputArray = new String[inputArgs.size()];
    newInputArray = inputArgs.toArray(newInputArray);
    //New job with the input array
    job.getConfiguration().setStrings(input,newInputArray);
    FileInputFormat.addInputPaths(job,  args[0]);
    FileOutputFormat.setOutputPath(job,  new Path(args[1]));
    job.setMapperClass( Map .class);
    job.setReducerClass( Reduce .class);
    job.setOutputKeyClass( Text .class);
    job.setOutputValueClass( DoubleWritable .class);
    return job.waitForCompletion( true)  ? 0 : 1;
 }
 
 public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {
    private final static IntWritable one  = new IntWritable( 1);
    private Text word  = new Text();
    private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
    public void map( LongWritable offset,  Text lineText,  Context context)
      throws  IOException,  InterruptedException {
        //Read line by line and set to String
       String line  = lineText.toString();     
       //Input is split into usable values with delimiter
       String[] Input = line.split("#####");       
       String word = Input[0].toString();    
      // The filename and TFIDF scores are split using delimiter
       String[] inputSplit = Input[1].split("\t");       
       String fName = inputSplit[0].toString();
       String tfidf1 = inputSplit[1].toString();      
       double tfidf2 = Double.parseDouble(tfidf1);       
       String[] temp = context.getConfiguration().getStrings(input);     
       
       
       for(String inputWord:temp){
      	 if(word.equalsIgnoreCase(inputWord))
      	 {
      		 // Written to context to give as input to reducer
      		context.write(new Text(fName),new DoubleWritable(tfidf2));
      		 
      	 } 
       }
    }
 }
 public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
    @Override 
    public void reduce( Text fileName,  Iterable<DoubleWritable > tfidfs,  Context context)
       throws IOException,  InterruptedException {      
  	  
  	  double tfidf3 = 0.0;	    	 
	    	 //Sum of TFIDF calculated
	         for ( DoubleWritable tfidf4  : tfidfs) {	            
	        	 tfidf3 = tfidf3 + tfidf4.get();        	 
	         } 
                 //Filename and TFIDF sum scores are displayed
       context.write(new Text(fileName),  new DoubleWritable(tfidf3));
    }
 }
}