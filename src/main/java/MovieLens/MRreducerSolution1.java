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

public class MRreducerSolution1  extends Reducer <Text,Text,Text,Text> {
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	
	   boolean firstTenValue=false;
	   int flagTenCount=0;
	   String [] movieDesc = new String [10];
	   int [] viewCount = new int[10];
	   int lowestCount= Integer.MAX_VALUE;
	   int lowestCountIndex = 0;
	   
	   while(values.iterator().hasNext()){
		   String singleMovie = values.iterator().next().toString();
		   String movieDesc_Ratings[] = singleMovie.split("@@KeyValueSepration@@");
		   if(movieDesc_Ratings.length==2){
			   movieDesc_Ratings[0] = movieDesc_Ratings[0].replaceAll("\\s+", "");
			   movieDesc_Ratings[0] = movieDesc_Ratings[0].replaceFirst("summary", "");
			   String movieRating[] = movieDesc_Ratings[1].split("@@MovieRatingSeperation@@");
				if(movieRating.length==2){
					String allRatings[] = movieRating[1].split("@::@");
					if(allRatings.length>0){
						String movieSummary[] = movieRating[0].split("@::@");
						if(!firstTenValue){
							movieDesc[flagTenCount]= movieSummary[0].replaceAll("MovieName_@_", "")+"@@@"+movieDesc_Ratings[0];
							viewCount[flagTenCount]=allRatings.length;
							flagTenCount++;
							if(flagTenCount==10){
								firstTenValue=true;
								lowestCount = lowestViewCount(viewCount);
								lowestCountIndex = lowestIndex(viewCount);
							}
								
						}
						else{
							if(allRatings.length>lowestCount){
								movieDesc[lowestCountIndex]= movieSummary[0].replaceAll("MovieName_@_", "")+"@@@"+movieDesc_Ratings[0];
								viewCount[lowestCountIndex] = allRatings.length;
								lowestCount = lowestViewCount(viewCount);
								lowestCountIndex = lowestIndex(viewCount);
							}
						}
					}
				}
		   }
	   }
	   context.write(new Text("Movie ID"), new Text("Movie Name"+"\t\t"+"Number of views"));
	   manageMovies(movieDesc, viewCount);
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
   private static void manageMovies(String []movies, int[] views){
	   for(int i=0; i<10; i++){
		   movies[i] = movies[i]+"@@@"+views[i];
	   }
	   Arrays.sort(movies);
   }
   private static int lowestViewCount(int[] arr){
	   int lowestCount=arr[0];
	   for(int i=1;i<10; i++){
		   if(arr[i]<lowestCount){
			   lowestCount = arr[i];
		   }
	   }
	   return lowestCount;
   }
   private static int lowestIndex(int[] arr ){
	   int lowestIndex=0;
	   int lowestCount=arr[0];
	   for(int i=1;i<10; i++){
		   if(arr[i]<lowestCount){
			   lowestCount = arr[i];
			   lowestIndex = i;
		   }
	   }
	   return lowestIndex;
   }
}
