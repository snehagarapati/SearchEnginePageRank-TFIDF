/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author sgara
 */
    


public class newPR extends Configured implements Tool {
    private final static IntWritable one = new IntWritable(1);

   private static final Logger LOG = Logger .getLogger( newPR.class);
	
   public static void main( String[] args) throws  Exception {
	  
      int res  = ToolRunner .run( new newPR(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " newPR ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( MapA .class);
      job.setReducerClass( ReduceA .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public class MapA extends Mapper<LongWritable, Text, Text, IntWritable> {

    Pattern DATA = Pattern.compile("\\[\\[(.*?)\\]\\]");
    Pattern TITLE = Pattern.compile("<title>(.*?)</title>");

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String line = value.toString();
        line = line.trim();
        String title = null;
        Matcher matcher2 = TITLE.matcher(line);
        if (matcher2.find()) {
            title = matcher2.group();
            title = title.trim();
            title = title.substring(7, title.length() - 8);
            context.write(new Text(title), one);
        } else {
//            context.write(new Text("no title"), value);
//            return;
        }

        List<String> outLinks = new ArrayList<>();
        Matcher matcher = DATA.matcher(line);
        while (matcher.find()) {
            String outlink = matcher.group();
            outlink = outlink.trim();
            outlink = outlink.substring(2, outlink.lastIndexOf(']') - 1);
            outLinks.add(outlink);
            context.write(new Text(outlink), one);
        }
    }

    
   }
   public class ReduceA extends Reducer<Text, IntWritable, Text, IntWritable> {

    int count = 0;

    protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws IOException, InterruptedException {
//        this.count++;
    }

    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        context.getCounter("Result", "Result").increment(count);
    }

}
}
