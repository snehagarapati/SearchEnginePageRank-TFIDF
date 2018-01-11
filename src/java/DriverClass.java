/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


//import RecommenderPackage.MapReduce1.Map1;
//import RecommenderPackage.Reducer1.Reduce1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class DriverClass{
/**
 *
 * @author swarna
 */
    
    public static void main(String arg0[]) throws IOException, InterruptedException, ClassNotFoundException{
  
		
		Configuration configuration  = new Configuration();
		//configuration.set("mapreduce.job.jar","CF.jar");
		
		Job job1 = Job.getInstance(configuration);
		job1.setJarByClass(DriverClass.class);

		job1.setJobName("InputFiles");
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputValueClass(Text.class);  
		job1.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job1, new Path(arg0[0])); //your path
   		FileOutputFormat.setOutputPath(job1, new Path(arg0[0]+"inter")); //your path
		
   		 job1.waitForCompletion(true);
                 
                 
                 
                 Job job2 = Job.getInstance(configuration);
		job2.setJarByClass(DriverClass.class);

		job2.setJobName("MergeFiles");
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
	job2.setOutputFormatClass(TextOutputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputValueClass(Text.class);  
		job2.setOutputKeyClass(Text.class);
		
		FileInputFormat.addInputPath(job2,new Path(arg0[0]+"inter")); //your path
   		FileOutputFormat.setOutputPath(job2, new Path(arg0[1])); //your path
		
   		 job1.waitForCompletion(true);
}

    


public static class Map1 extends Mapper<Object,Object,Text, Text> {
    
     private String Flagtag_moviename="MN~";
     private String Flagtag_userid="U~";
     
        protected void map(Object key, Object value, Mapper<Object,Object,Text, Text>.Context context)
			throws IOException, InterruptedException {
				
	FileSplit inputSplit =  (FileSplit) context.getInputSplit();
	String path = inputSplit.getPath().toString();
	int one=1;
	
	if(path.contains("movies")){
		
		String[] split = value.toString().split(",");		
		String movieid = split[0];
		String moviename=split[1];
		               
		context.write(new Text(movieid),new Text(Flagtag_moviename+moviename));
	}
	if(path.contains("ratings")) {
		
		String[] split = value.toString().split(",");		
		String movieid = split[1];
		String userid=split[0];
		String rating=split[2];
				               
		context.write(new Text(movieid), new Text(Flagtag_userid+userid+","+rating));
	}	
}	
}    

public static class Reduce1 extends Reducer<Text,Text,Text,Text>
{
    
    private String movie_name="";
  private String user_id="";
  private String rating="";
  private String user_rating_pair="";
protected void reduce(Text key,Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context)
			throws IOException, InterruptedException {
		
		System.out.println("Hey");//!= null ? splitVals[1].trim(): "moviename";

                Iterator<Text> iter=values.iterator();
                
                StringBuilder allUserMovieRates = new StringBuilder();
                
                for(Text val:values)
                {
                    
                    String[] splitVals = val.toString().split("~");
                    if(splitVals[0].equals("MN"))
                    {
                        movie_name = splitVals[1].trim(); 
                        ArrayList ar=new ArrayList<String>();
                        ar.add(movie_name);     
                    }
                    else if(splitVals[0].equals("U"))
                    {
                        user_rating_pair=splitVals[1].trim();
                        
                         String[] split=user_rating_pair.split(",");
                         user_id=split[0];
                          rating=split[1];
                         //user_rating_pair.replaceAll("",movie_name);            
                    }
                     allUserMovieRates.append(user_id+","+movie_name+","+rating+"~");           
                }
                context.write(key, new Text(allUserMovieRates.toString().trim()));
}
}


public static class Map2 extends Mapper<Object,Object,Text, Text> {
   
   
        protected void map(Object key, Object value, Mapper<Object,Object,Text, Text>.Context context)
			throws IOException, InterruptedException {
				
		
		String[] user_rating_pair = value.toString().split("~");
                for(int i = 0 ; i < user_rating_pair.length; i++){
                String[] usermovierate= user_rating_pair[i].split(",");
                String user=usermovierate[0];
                String movie=usermovierate[1];
                String rate=usermovierate[2];
                
                context.write(new Text(user),new Text(movie+rate));
                }
        }
}

public static class Reduce2 extends Reducer<Text,Text,Text,Text>
{
   
protected void reduce(Text key,Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuilder allUserMovieRates = new StringBuilder();
		
		for(Text val : values){
			
			allUserMovieRates.append(val + " " );
		}
		
		context.write(key, new Text(allUserMovieRates.toString().trim()));
}
}		            

}





		/* StringBuilder allUserMovieRates = new StringBuilder();
		 for(Text val : values){
			
			allUserMovieRates.append(val+ " " );
		}
                
                
                
                
                */ /*
                 while (iter.hasNext()) {
                     System.out.println("Hello");//!= null ? splitVals[1].trim(): "moviename";

               StringBuilder UserMovieRates = new StringBuilder();
               StringBuilder newbuilder = new StringBuilder();
               Text val = iter.next();
    String currValue = iter.next().toString();
    String[] splitVals = currValue.split("~");
  
  if (splitVals[0].equals("MN")) {
     movie_name = splitVals[1].trim(); 
System.out.println(movie_name);//!= null ? splitVals[1].trim(): "moviename";
  } else if (splitVals[0].equals("U")) {
      // getting the file2 and using the same to obtain the Message
      user_id_rating = splitVals[1].trim();
  }// != null ? splitVals[2].trim(): "id_rating";
    UserMovieRates.append(user_id_rating+"");  
    
    String[] split=UserMovieRates.toString().split(",");
    user_id=split[0];
    rating=split[1];
    newbuilder.append(movie_name+rating+"");
    
    context.write(new Text(user_id), new Text(newbuilder.toString().trim()));  
    
                
       /*
             
           while (values.hasNext()) {
               StringBuilder UserMovieRates = new StringBuilder();
    String currValue = values.next().toString();
    String splitVals[] = currValue.split("~");
  
  if (splitVals[0].equals("MN")) {
     movie_name = splitVals[1]; 
System.out.println(movie_name);//!= null ? splitVals[1].trim(): "moviename";
  } else if (splitVals[0].equals("U")) {
      // getting the file2 and using the same to obtain the Message
      user_id_rating = splitVals[2].trim();// != null ? splitVals[2].trim(): "id_rating";
      
      String[] split = user_id_rating.toString().split(",");
      
      user_id=split[0];
      System.out.println(user_id);
      rating=split[1];
      System.out.println(rating);
  }
  UserMovieRates.append(movie_name+rating+"");
context.write(new Text(user_id), new Text(UserMovieRates.toString().trim()));  
 
 }
}
}
   */      

