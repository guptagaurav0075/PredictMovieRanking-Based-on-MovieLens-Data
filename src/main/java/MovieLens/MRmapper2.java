package MovieLens;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MRmapper2  extends Mapper <LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
	// TODO: write (key, value) pair to context (hint: need to be clever here)
		String[] vals = value.toString().split("@@KeyValueSepration@@");
		if(vals.length==2){
			vals[0]=vals[0].replaceAll("\\s+", "");
			String ratingMovie = vals[1];
			String desc[] = ratingMovie.split("@@Movie-RatingSepration@@");
			if(desc.length==2){
				desc[0]= desc[0].replaceFirst("@::@", "");
				desc[1] = desc[1].replaceFirst("@::@", "");
				StringBuffer combinedValue = new StringBuffer();
				combinedValue.append(vals[0]);
				combinedValue.append("@@KeyValueSepration@@");
				combinedValue.append(desc[0]);
				combinedValue.append("@@MovieRatingSeperation@@");
				combinedValue.append(desc[1]);
				context.write(new Text("summary"), new Text(combinedValue.toString()));
			}
		}
	}
}
