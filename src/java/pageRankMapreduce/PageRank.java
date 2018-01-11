/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pageRankMapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

/**
 *
 * @author sgara
 */
public class PageRank {
    
    public static void main(String[] args) throws Exception {
        PageRank pageRanking = new PageRank();
 
        //In and Out dirs in HDFS
        pageRanking.runXmlParsing("C:\\Users\\sgara\\Documents\\Courses_Fall2016\\CloudComputing\\Assignment3\\wiki-micro.txt\\wiki-micro.txt", "C:\\Users\\sgara\\Documents\\Courses_Fall2016\\CloudComputing\\Assignment3\\test1.txt");
    }

    private void runXmlParsing(String inputPath, String outputPath) {
        JobConf job = new JobConf(PageRank.class);
                FileInputFormat.setInputPaths(job, new Path(inputPath));
                
job.setInputFormat(XmlInputFormat.class);
                
