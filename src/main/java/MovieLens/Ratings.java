package MovieLens;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 */

public class Ratings extends Mapper <LongWritable,Text,Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO: filter failed USER_LOGIN records, discard the rest
		String Line = value.toString();
		String[] values = Line.split("::");
		if(values.length==4){
			StringBuffer movieID =  new StringBuffer();
			movieID.append("MovieID_@_");
			movieID.append(values[1]);
			StringBuffer ratingDesc = new StringBuffer();
			
			ratingDesc.append("UserID_@_");
			ratingDesc.append(values[0]);
			ratingDesc.append("::");
			ratingDesc.append("Rating_@_");
			ratingDesc.append(values[2]);
			ratingDesc.append("::");
			ratingDesc.append("TimeStamp_@_");
			ratingDesc.append(values[3]);
			context.write(new Text(movieID), new Text(ratingDesc.toString()));
		}
	}
}