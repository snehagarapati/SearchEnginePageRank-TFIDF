
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
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
public class TFIDF extends Configured implements Tool {
    int filecount;

    private static final Logger LOG = Logger.getLogger(TFIDF.class);
    //String for fileCount is taken
    private static final String fileCount = "";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TFIDF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        String outputDir = "";
        //New job for TFIDF
        Job job1 = Job.getInstance(getConf(), "TFIDF");
        job1.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + outputDir));
        filecount++;
      job1.setNumReduceTasks(filecount);
        job1.setMapperClass(MapA.class);
        job1.setReducerClass(ReduceA.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        boolean success = job1.waitForCompletion(true);
//If job is successful the filecount and TFIDF are further calculated
        if (success) {
            FileSystem filesystem = FileSystem.get(getConf());
            Path path = new Path(args[0]);
            ContentSummary content = filesystem.getContentSummary(path);
            long filecount = content.getFileCount();
            getConf().set(fileCount, "" + filecount);
//New Job for TFIDF calculation
            Job job2 = Job.getInstance(getConf(), " TFIDF ");
            job2.setJarByClass(this.getClass());

            job2.setMapperClass(MapB.class);
            job2.setReducerClass(ReduceB.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1] + outputDir));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.waitForCompletion(true);

            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return job1.waitForCompletion(true) ? 0 : 1;
    }
//  First mapper for wf calculation
    public static class MapA extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        String fName = new String();
        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            String fname = ((FileSplit) context.getInputSplit()).getPath().getName();

            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }

                StringBuilder result = new StringBuilder();

                result.append(word).append("#####").append(fName);

                
                context.write(new Text(result.toString()), one);
            }
        }

    }
//First Reducer for wf calculation
    public static class ReduceA extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            DoubleWritable wfvalue = new DoubleWritable( 1 +Math.log10((double) sum));
            context.write(word, wfvalue);
        }
    }
//Second Mapper to calculate TFIDF from wf in previous mapper
    public static class MapB extends Mapper<LongWritable, Text, Text, Text> {

               private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            if (line.isEmpty()) {
                return;
            }
            //The string input contains word, filename. Splitting using delimiter
            String[] argsSplit1 = line.split("#####");
//Input String is split into word and filename with tfidf score using array split
            Text word1 = new Text(argsSplit1[0]);

            Text FnameWF = new Text(argsSplit1[1]);
            String[] argsSplit2 = (FnameWF.toString()).split("\t");
//Filename and wf are now split into each using another array split
            Text argswordname = new Text(argsSplit2[0]);

            Text argsfname = new Text(argsSplit2[1]);

            StringBuilder addString = new StringBuilder();

            addString.append(argswordname.toString()).append("&").append(argsfname.toString());

           

            context.write(word1, new Text(addString.toString()));

        }
    }
//Second Reducer to give TFIDF score output
    public static class ReduceB extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double Idf = 0;
            double TfIdf = 0;
            long filecount1 = context.getConfiguration().getLong(fileCount, 1);
            ArrayList<Text> resultArray = new ArrayList<Text>();

            for (Text val : values) {
                resultArray.add(new Text(val.toString()));
            }
            double arraysize = resultArray.size();

            Idf = (Math.log10(1 + (filecount1 / arraysize)));

            for (int i = 0; i < arraysize; i++) {
                String temp = resultArray.get(i).toString();
                String[] temparray = temp.split("&");
//the File name and wf scores are split using the array and taken as input
                Text fNameFromInput = new Text(temparray[0]);
                Text wfScore = new Text(temparray[1]);

                double d = Double.parseDouble(wfScore.toString());
                StringBuilder finalWordAndFName = new StringBuilder();
                //the Final word input and filename and added to a string
                finalWordAndFName.append(word.toString()).append("#####").append(fNameFromInput.toString());
                //TFIDF is calculated using the wf score
                TfIdf = Double.parseDouble(wfScore.toString()) * Idf;

                String finalOutput = finalWordAndFName.toString();
                
                context.write(new Text(finalOutput), new DoubleWritable(TfIdf));

            }

        }
    }
}
