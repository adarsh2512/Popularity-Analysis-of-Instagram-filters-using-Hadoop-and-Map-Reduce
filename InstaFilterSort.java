package defaultpackage;



import java.io.IOException;
import java.util.*; 	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 	
public class InstaFilterSort {

	public static class Map
	extends Mapper<LongWritable, Text, LongWritable, Text>{

		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			String temp = value.toString();
			String[] oneRowOfInput = temp.split("\t");
			context.write(new LongWritable(Long.parseLong(oneRowOfInput[1])), new Text(oneRowOfInput[0]));
		}
	}

	public static class Reduce
	extends Reducer<LongWritable, Text, Text, LongWritable> {

		public void reduce(LongWritable key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			
			for (Text t : values) {
                context.write(new Text(t + "\t" + key),null);
            }
		}
	}
	
	public static class instaSortComparator extends WritableComparator {

        protected sortComparator() {
         super(LongWritable.class, true);
        }
        public int compare(WritableComparable x, WritableComparable y) {
         LongWritable i = (LongWritable) x;
         LongWritable j = (LongWritable) y;
         int compare = i.compareTo(j);
         return -1 * compare;
        }

     }

	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		JobConf con = new JobConf(InstaFilterSort.class);
        Job job = new Job(conf, "InstaFilterSort");
        job.setJarByClass(InstaFilterSort.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(instaSortComparator.class);
        con.setInputFormat(TextInputFormat.class);
        con.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
