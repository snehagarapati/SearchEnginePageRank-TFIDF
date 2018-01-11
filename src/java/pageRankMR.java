package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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


public class pageRankMR extends Configured implements Tool {

   
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new pageRankMR(), args);
      System .exit(res);
    }
    
      //Job1 has to parse the XML and identify the pattern

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), "pageRankMR");
      job.setJarByClass( this .getClass());
       setSeparator(job.getConfiguration());
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[0] + "taskNum"));
      job.setMapperClass( XmlPatternMapper .class);
      job.setReducerClass( XmlPatternReducer .class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);
      job.waitForCompletion( true);
      //Takes the counter on the reducer and gives the number of nodes
      long N = job.getCounters().findCounter("Output", "Output").getValue();

       //Job2 has the 1st output taking Num of nodes as an input 
        Job job2 = Job.getInstance(getConf(), "LinkGraph");
        job2.setJarByClass(this.getClass());
        //Delimiter is being set with the nodes
        setSeparator(job2.getConfiguration());
        
        FileInputFormat.addInputPaths(job2, args[0]);
        //Saving the output files for next job
        FileOutputFormat.setOutputPath(job2, new Path(args[0] + "iter0"));
        
        job2.getConfiguration().set("InputFiles", "" + N);
        job2.setMapperClass(LinkGraphMapper.class);
        job2.setReducerClass(LinkGraphReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setMapOutputValueClass(Text.class);
        job2.waitForCompletion(true);
        Job job3;
        //Set the iterations to iterations = 10
        for (int i = 1; i <= iterations; i++) {
            job3 = Job.getInstance(getConf(), "PageRank" + i);
            job3.setJarByClass(this.getClass());
            setSeparator(job3.getConfiguration());
            //Each output is taken as the next input
            FileInputFormat.addInputPaths(job3, args[0] + "iter" + (i - 1));
            FileOutputFormat.setOutputPath(job3, new Path(args[0] + "iter" + i));
            //Final output is iter10
            job3.setMapperClass(PageRankMapper.class);
            job3.setReducerClass(PageRankReducer.class);
            job3.setOutputFormatClass(TextOutputFormat.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.waitForCompletion(true);
        }
        //This job has the pageranks and sorts accodringly and saves in the output file by taking the last iter10 as the input
        Job job4 = Job.getInstance(getConf(), "Sorting");
        job4.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job4, args[0] + "iter10");
        FileOutputFormat.setOutputPath(job4, new Path(args[1]));
        job4.setNumReduceTasks(1);
        job4.getConfiguration().set("InputFiles", "" + N);
        job4.setMapperClass(SortingMapper.class);
        job4.setReducerClass(SortingReducer.class);
        job4.setMapOutputKeyClass(DoubleWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.waitForCompletion(true);

return 1;

   }

public static class XmlPatternMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//Patterns for Title and Outlinks
    Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
        private static final Pattern OutLinkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]");
            private final static IntWritable one = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String page = value.toString();
        page = page.trim();
        String title = null;
        //matching the pattern and grouping the outputs
        Matcher titleMatcher = titlePattern.matcher(page);
        if (titleMatcher.find()) {
            //if match found, group the titles
            title = titleMatcher.group();
            title = title.trim();
            title = title.substring(7, title.length() - 8);
            context.write(new Text(title), one);
        }
//Outlinks of all nodes
        List<String> outLinksList = new ArrayList<>();
        Matcher outLinkMatcher = OutLinkPattern.matcher(page);
        while (outLinkMatcher.find()) {
            String outLink = outLinkMatcher.group();
            outLink = outLink.trim();
            outLink = outLink.substring(2, outLink.lastIndexOf(']') - 1);
            outLinksList.add(outLink);
            context.write(new Text(outLink), one);
        }
    }

}
public static class XmlPatternReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    int count = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        this.count++;
    }
    //Number of times reducer is run, the number of nodes are counted. Returns count = Number of Nodes
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.getCounter("Output", "Output").increment(count);
    }
   
}
public static class LinkGraphMapper extends Mapper<LongWritable, Text, Text, Text> {

    


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    
        String page = value.toString();
        page = page.trim();
        String title;
        Matcher titleMatcher = TitlePattern.matcher(page);
        if (titleMatcher.find()) {
            title = titleMatcher.group();
            title = title.trim();
            title = title.substring(7, title.length() - 8);
        } else {
            return;
        }

        List<String> outLinksList = new ArrayList<>();
        Matcher outLinkPattern = outLinksPattern.matcher(page);
        while (outLinkPattern.find()) {
            String outlink = outLinkPattern.group();
            outlink = outlink.trim();
            outlink = outlink.substring(2, outlink.lastIndexOf(']') - 1);
            outLinksList.add(outlink);
        }
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < outLinksList.size(); i++) {
            String data = outLinksList.get(i);
            buffer.append(data);
            if (i != outLinksList.size() - 1) {
                buffer.append(linkDelim);
            }
        }

        context.write(new Text(title), new Text(buffer.toString()));
    }
Pattern TitlePattern = Pattern.compile("<title>(.*?)</title>");
        private static final Pattern outLinksPattern = Pattern.compile("\\[\\[.*?\\]\\]");
}
public static class LinkGraphReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context); 
        initialRank = context.getConfiguration().getDouble("InputFiles", 0);
        initialRank = 1.0 / initialRank;
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        Iterator<Text> iterat = values.iterator();
        while (iterat.hasNext()) {
            Text value = iterat.next();
            sb.append(value.toString());
                sb.append(linkDelim);
        }
        context.write(key, new Text(initialRank + OutLinks + sb.toString()));

    }
    double initialRank = 0;

}
public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] outlinkswithsep = value.toString().split(nodeSeperator);
        String page = outlinkswithsep[0];
        outlinkswithsep = outlinkswithsep[1].split(OutLinks);
        double rank_1 = Double.parseDouble(outlinkswithsep[0]);
        String outlinks = "";
        if (outlinkswithsep.length > 1) {
            outlinks = outlinkswithsep[1];
            outlinkswithsep = outlinkswithsep[1].split(linkDelim);
            double pagerank = rank_1 / outlinkswithsep.length;
            for (String d : outlinkswithsep) {
                context.write(new Text(d), new Text("" + pagerank));
            }
        }
        context.write(new Text(page), new Text(outlinks));
    }
    private final static IntWritable one = new IntWritable(1);

}
public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double pagerank = 0;
        String outlinks = "";
        for (Text value : values) {
            if (value.toString().contains(linkDelim)) {
                outlinks = value.toString();
            } else if(!value.toString().equals("")){
                pagerank += Double.parseDouble(value.toString());
            }
        }
        pagerank = .15 + .85*pagerank;
        context.write(key, new Text(pagerank + OutLinks + outlinks));
    }

}

public static class SortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
      private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] temp = value.toString().split(nodeSeperator);
        String page = temp[0];
        temp = temp[1].split(OutLinks);
        double pagerank = (-1) * Double.parseDouble(temp[0]);
        //Giving the key as pagerank and node as value
        context.write(new DoubleWritable(pagerank), new Text(page));
    }
}
public static class SortingReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable>  {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         double finalRank = (-1) * Double.parseDouble(key.toString());
         for (Text num : values)
         {
             //Reversing the values for right output as key: node, value: finaloutput
              context.write(new Text(num.toString()), new DoubleWritable(finalRank));
         }
    }
}

//Delimiters and iterations declarations
int iterations = 10;
private static final Logger LOG = Logger .getLogger( pageRankMR.class);
   public static String nodeSeperator = "#&&&&#";
   public static String linkDelim = "##&&&##";
   public static String OutLinks = "##&&&OutLinks&&##";
   
   private void setSeparator(Configuration conf) {
        conf.set("mapred.textoutputformat.separator", nodeSeperator); 
   }

}
