package MovieLens;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 */

public class Movies extends Mapper <LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: filter failed USER_LOGIN records, discard the rest
		String Line = value.toString();
		String[] values = Line.split("::");
		if(values.length==3){
			StringBuffer movieID =  new StringBuffer();
			movieID.append("MovieID_@_");
			movieID.append(values[0]);
			StringBuffer movieDesc = new StringBuffer();
			movieDesc.append("MovieName_@_");
			movieDesc.append(values[1]);
			movieDesc.append("::");
			movieDesc.append("Genres_@_");
			movieDesc.append(values[2]);
			context.write(new Text(movieID.toString()), new Text(movieDesc.toString()));
		}
	}
}
