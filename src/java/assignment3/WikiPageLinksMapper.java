/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package assignment3;

import static com.sun.corba.se.impl.util.RepositoryId.cache;
import static com.sun.org.apache.xalan.internal.lib.ExsltStrings.align;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author sgara
 */
public class WikiPageLinksMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
 
    private static final Pattern wikiLinksPattern = Pattern.compile("\\<p style=\"text-align:center;\"><span class=\"MathJax_Preview\"><img src=\"http://blog.xebia.com/wp-content/plugins/latex/cache/tex_566959e6dfa4654717b565faf2095be8.gif\" style=\"vertical-align: middle; border: none;\" class=\"tex\" alt=\".+?\\\"></span><script type=\"math/tex;  mode=display\">.+?\\</script></p>\"");
 
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

