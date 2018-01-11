 /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment3;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.hadoop.mapreduce.*
//import org.apache.hadoop.mapreduce.;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.mahout.classifier.bayes.*;


/**
 *
 * @author sgara
 */
public class PageRankMapReduce {
 
    
        /**
         *
         * @param args
         * @throws Exception
         */
        public static void main(String[] args) throws Exception {
        PageRankMapReduce pageRanking = new PageRankMapReduce();
 
        //In and Out dirs in HDFS
        pageRanking.runXmlParsing("wikipageinput", "wikipageoutput");
    }
 
    public void runXmlParsing(String inputPath, String outputPath) throws IOException {
        JobConf job2 = new JobConf(PageRankMapReduce.class);
 
        FileInputFormat.setInputPaths(job2, new Path(inputPath));
        // Mahout class to Parse XML + job2ig
        job2.setInputFormat(XmlInputFormat.class);
        job2.set(XmlInputFormat.START_TAG_KEY, "<page>");
        job2.set(XmlInputFormat.END_TAG_KEY, "</page>");
        // Our class to parse links from content.
        job2.setMapperClass(WikiPageLinksMapper.class);
 
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));
        job2.setOutputFormat(TextOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        // Our class to create initial output
        job2.setReducerClass(WikiLinksReducer.class);
        //.addFileToClassPath(new Path("D:\\JarFiles\\mahout-examples-0.4.jar"), job2);
 
        JobClient.runJob(job2);
    }
    
    public class WikiLinksReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String pagerank = "1.0\t";
 
        boolean first = true;
        while(values.hasNext()){
            if(!first) pagerank += ",";
 
            pagerank += values.next().toString();
            first = false;
        }
 
        output.collect(key, new Text(pagerank));
    }


    

    public void Configure(JobConf jc) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }


    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
    
    public class WikiPageLinksMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 
            /**
             *
             */
            private final Pattern wikiLinksPattern = Pattern.compile("\\<p style=\"text-align:center;\"><span class=\"MathJax_Preview\"><img src=\"http://blog.xebia.com/wp-content/plugins/latex/cache/tex_566959e6dfa4654717b565faf2095be8.gif\" style=\"vertical-align: middle; border: none;\" class=\"tex\" alt=\".+?\\\"></span><script type=\"math/tex;  mode=display\">.+?\\</script></p>\"");
 
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Returns  String[0] = <title>[TITLE]</title>
        //          String[1] = <text>[CONTENT]</text>
        // !! without the <tags>.
        String[] titleAndText = parseTitleAndText(value);
 
        String pageString = titleAndText[0];
        Text page = new Text(pageString.replace(' ', '_'));
 
        Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);
 
        //Loop through the matched links in [CONTENT]
        while (matcher.find()) {
            String otherPage = matcher.group();
            //Filter only wiki pages.
            //- some have [[realPage|linkName]], some single [realPage]
            //- some link to files or external pages.
            //- some link to paragraphs into other pages.
            otherPage = getWikiPageFromLink(otherPage);
            if(otherPage == null || otherPage.isEmpty())
                continue;
 
            // add valid otherPages to the map.
            output.collect(page, new Text(otherPage));
        }
    }
 
    //... the impl of parsePageAndText(..)
    //... the impl of getWikiPageFromLink(..)

    private String getWikiPageFromLink(String otherPage) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private String[] parseTitleAndText(Text value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    }


    }



