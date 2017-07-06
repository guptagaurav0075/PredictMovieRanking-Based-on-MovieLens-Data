package MovieLens;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.lang.Math;

public class MRreducerSolution2  extends Reducer <Text,Text,Text,Text> {
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	
	   boolean firstTwentyValue=false;
	   int flagTwentyCount=0;
	   String [] movieDesc = new String [20];
	   double [] avgCount = new double[20];
	   double lowestAvg= Double.MAX_VALUE;
	   int lowestAvgIndex = 0;
	   
	   while(values.iterator().hasNext()){
		   String singleMovie = values.iterator().next().toString();
		   String movieDesc_Ratings[] = singleMovie.split("@@KeyValueSepration@@");
		   if(movieDesc_Ratings.length==2){
			   movieDesc_Ratings[0] = movieDesc_Ratings[0].replaceAll("\\s+", "");
			   movieDesc_Ratings[0] = movieDesc_Ratings[0].replaceFirst("summary", "");
			   String movieRating[] = movieDesc_Ratings[1].split("@@MovieRatingSeperation@@");
				if(movieRating.length==2){
					double avg = avgRating(movieRating[1]);
					if(avg<0)
						continue;
					String movieSummary[] = movieRating[0].split("@::@");
					if(!firstTwentyValue){
						movieDesc[flagTwentyCount]= movieSummary[0].replaceAll("MovieName_@_", "")+"@@@"+movieDesc_Ratings[0];
						avgCount[flagTwentyCount]=avg;
						flagTwentyCount++;
						if(flagTwentyCount==20){
							firstTwentyValue=true;
							lowestAvg = lowestAvgMovie(avgCount);
							lowestAvgIndex = lowestIndex(avgCount);
						}
					}
					else{
						if(avg>lowestAvg){
							movieDesc[lowestAvgIndex]= movieSummary[0].replaceAll("MovieName_@_", "")+"@@@"+movieDesc_Ratings[0];
							avgCount[lowestAvgIndex] = avg;
							lowestAvg = lowestAvgMovie(avgCount);
							lowestAvgIndex = lowestIndex(avgCount);
						}
					}
				}
		   }
	   }
	   context.write(new Text("Movie ID"), new Text("Movie Name"+"\t\t"+"Average Rating"));
	   manageMovies(movieDesc, avgCount);
	   for(int i=0; i<movieDesc.length; i++){
		   String [] movies = movieDesc[i].split("@@@");
		   if(movies.length==3){
			   String movieName[] = movies[0].split("::Genres");
			   if(movieName.length>0){
				   context.write(new Text(movies[1]), new Text(movieName[0]+"\t"+movies[2]));   
			   }
		   }
	   }
   }
   private static double avgRating(String ratings){
	   double avgRating = 0;
	   String allRatings[] = ratings.split("@::@");
	   if(allRatings.length<40){
		   return -1;
	   }
	   else{
		   int totalNumberOfRatings = allRatings.length;
		   double sum = 0;
		   for(int i =0; i<allRatings.length; i++){
			   String allDatas[] = allRatings[i].split("::");
			   if(allDatas.length>0){
				   String RatingData[] = allDatas[1].split("_@_");
				   if(RatingData.length>0){
					   sum+= Integer.parseInt(RatingData[1]);
				   }
			   }
		   }
		   return (double)sum/(double)totalNumberOfRatings;
	   }
   }
   private static void manageMovies(String []movies, double[] avgs){
	   for(int i=0; i<20; i++){
		   movies[i] = movies[i]+"@@@"+avgs[i];
	   }
	   Arrays.sort(movies);
   }
   private static double lowestAvgMovie(double[] arr){
	   double lowestCount=arr[0];
	   for(int i=1;i<20; i++){
		   if(arr[i]<lowestCount){
			   lowestCount = arr[i];
		   }
	   }
	   return lowestCount;
   }
   private static int lowestIndex(double[] arr ){
	   int lowestIndex=0;
	   double lowestCount=arr[0];
	   for(int i=1;i<20; i++){
		   if(arr[i]<lowestCount){
			   lowestCount = arr[i];
			   lowestIndex = i;
		   }
	   }
	   return lowestIndex;
   }
}
