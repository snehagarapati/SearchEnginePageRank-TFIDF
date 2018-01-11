package org.myorg;


import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author sgara
 */

public class pageRankMR extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( pageRankMR.class);
   

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new pageRankMR(), args);
      System .exit(res);
    }
    

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), "pagerankmr");
      job.setJarByClass( this .getClass());

       setSeparator(job.getConfiguration());
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[0] + "taskNum"));
      job.setMapperClass( XmlLinkMapper .class);
      job.setReducerClass( XmlLinkReducer .class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);
      job.waitForCompletion( true);
      NumofNodes = job.getCounters().findCounter("Output", "Output").getValue();

        Job job2 = Job.getInstance(getConf(), "LinkMap");
        job2.setJarByClass(this.getClass());
        setSeparator(job2.getConfiguration());
        FileInputFormat.addInputPaths(job2, args[0]);
        FileOutputFormat.setOutputPath(job2, new Path(args[0] + "iter0"));
        job2.getConfiguration().set("TotalNodes", "" + NumofNodes);
        job2.setMapperClass(LinkGraphMapper.class);
        job2.setReducerClass(LinkGraphReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapOutputValueClass(Text.class);
        job2.waitForCompletion(true);
        
        
        Job job3;
        for (int i = 1; i <= iterations ; i++) {
 
            job3 = Job.getInstance(getConf(), "ranker" + i);
            job3.setJarByClass(this.getClass());
            setSeparator(job3.getConfiguration());
            FileInputFormat.addInputPaths(job3, args[0] + "iter" + (i - 1));
            FileOutputFormat.setOutputPath(job3, new Path(args[0] + "iter" + i));
            job3.setMapperClass(PageRankMapper.class);
            job3.setReducerClass(PageRankReducer.class);
            job3.setOutputFormatClass(TextOutputFormat.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.waitForCompletion(true);
        }
        
        Job job4 = Job.getInstance(getConf(), "sort");
        job4.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job4, args[0] + "iter10");
        FileOutputFormat.setOutputPath(job4, new Path(args[1]));
       job4.setNumReduceTasks(1);
        job4.getConfiguration().set("TotalNodes", "" + NumofNodes);
        job4.setMapperClass(CleanupMapper.class);
        job4.setReducerClass(CleanupReducer.class);
        job4.setMapOutputKeyClass(DoubleWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.waitForCompletion(true);

return 1;

   }

public static class XmlLinkMapper extends Mapper<LongWritable, Text, Text, IntWritable> {    

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String page = value.toString();
        page = page.trim();
        String titles = null;
        Matcher titleMatcher = titlePattern.matcher(page);
        if (titleMatcher.find()) {
            titles = titleMatcher.group();
            titles = titles.trim();
            titles = titles.substring(7, titles.length() - 8);
            context.write(new Text(titles), one);
        }

        List<String> outLinksList = new ArrayList<>();
        Matcher outLinkMatcher = outLinkPattern.matcher(page);
        while (outLinkMatcher.find()) {
            String outLink = outLinkMatcher.group();
            outLink = outLink.trim();
            outLink = outLink.substring(2, outLink.lastIndexOf(']') - 1);
            outLinksList.add(outLink);
            context.write(new Text(outLink), one);
        }
    }
    Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
    private static final Pattern outLinkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
    private final static IntWritable one = new IntWritable(1);
}
public static class XmlLinkReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    int counter = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        this.counter++;
    }
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("Result", "Result").increment(counter);
    }
   
}
public static class LinkGraphMapper extends Mapper<LongWritable, Text, Text, Text> {

    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String page = value.toString();
        page = page.trim();
        String titles;
        Matcher titleMatcher = titlePattern.matcher(page);
        if (titleMatcher.find()) {
            titles = titleMatcher.group();
            titles = titles.trim();
            titles = titles.substring(7, titles.length() - 8);
        } else {
            return;
        }

        List<String> outLinksList = new ArrayList<>();
        Matcher outLinkMatcher = outLinkPattern.matcher(page);
        while (outLinkMatcher.find()) {
            String outlinks = outLinkMatcher.group();
            outlinks = outlinks.trim();
            outlinks = outlinks.substring(2, outlinks.lastIndexOf(']') - 1);
            outLinksList.add(outlinks);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < outLinksList.size(); i++) {
            String data = outLinksList.get(i);
            sb.append(data);
            if (i != outLinksList.size() - 1) {
                sb.append(linkDelim);
            }
        }

        context.write(new Text(titles), new Text(sb.toString()));
    }
Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
    private static final Pattern outLinkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
    private final static IntWritable one = new IntWritable(1);
}
public static class LinkGraphReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        intialRank = context.getConfiguration().getDouble("TotalNodes", 0);
        intialRank = 1.0 / intialRank;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb1 = new StringBuilder();
        Iterator<Text> itera = values.iterator();
        while (itera.hasNext()) {
            Text value = itera.next();
            sb1.append(value.toString());
                sb1.append(linkDelim);
        }
        context.write(key, new Text(intialRank + outLinkSeperator + sb1.toString()));

    }
    double intialRank = 0;

}
public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        
        String[] links = "";        
        links = value.toString().split(nodeSeperator);
        String page = links[0];
        links = links[1].split(outLinkSeperator);
        rank1 = Double.parseDouble(data[0]);
        String outlinks = "";
        if (links.length > 1) {
            outlinks = links[1];
            links = links[1].split(linkDelim);
            double pageRank = rank / links.length;
            for (String d : links) {
                context.write(new Text(d), new Text("" + pageRank));
            }
        }
        context.write(new Text(page), new Text(outlinks));
        
    }
double rank1 = 0;
}
public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String outlinks = "";
        for (Text value : values) {
            if (value.toString().contains(linkDelim)) {
                outlinks = value.toString();
            } else if(!value.toString().equals("")){
                rank += Double.parseDouble(value.toString());
            }
        }
        rank = .15 + .85*rank;
        context.write(key, new Text(rank + outLinkSeperator + outlinks));
    }
        double rank = 0;

}

public static class SortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(nodeSeperator);
        String page = data[0];
        data = data[1].split(outLinkSeperator);
        double rank = (-1) * Double.parseDouble(data[0]);
        context.write(new DoubleWritable(rank), new Text(page));
    }
          private final static IntWritable one = new IntWritable(1);

    }

public static class SortingReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>  {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
         double finalRank = 0;
         finalRank = (-1) * Double.parseDouble(key.toString());
         for (Text cnt : values)
         {
              context.write(new Text(cnt.toString()), new DoubleWritable(finalRank));
         }
    }
}
int iterations = 10;
long NumofNodes = 0;
public static String nodeSeperator = "######";
   public static String linkDelim = "@@@@@@";
   public static String outLinkSeperator = "&&&outLinks&&&";
   
   private void setSeparator(Configuration conf) {
        conf.set("mapred.textoutputformat.separator", nodeSeperator); 
        conf.set("mapreduce.textoutputformat.separator", nodeSeperator);  
   }
}
