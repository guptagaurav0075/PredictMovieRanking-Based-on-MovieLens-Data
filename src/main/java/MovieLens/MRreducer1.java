package MovieLens;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.Iterator;

public class MRreducer1  extends Reducer <Text,Text,Text,Text> {
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	// TODO: calculate total failed logins per user and write to context
	   StringBuffer MovieDesc = new StringBuffer();
	   StringBuffer RatingDesc = new StringBuffer();
	   while(values.iterator().hasNext()){
		   String Line = values.iterator().next().toString();
		   String vals[] = Line.split("::");
		   if(vals.length==2){
			   MovieDesc.append("@::@");
			   MovieDesc.append(Line);
			   
		   }
		   else if(vals.length==3){
			   RatingDesc.append("@::@");
			   RatingDesc.append(Line);
			   
		   }
	   }
	   StringBuffer combinedValue = new StringBuffer();
	   combinedValue.append("@@KeyValueSepration@@");
	   combinedValue.append(MovieDesc.toString());
	   combinedValue.append("@@Movie-RatingSepration@@");
	   combinedValue.append(RatingDesc.toString());
	   context.write(key, new Text(combinedValue.toString()));
   }
}
