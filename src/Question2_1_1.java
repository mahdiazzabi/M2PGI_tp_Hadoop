
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question2_1_1 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] ligne = value.toString().split("\\t");
			String[] tags = ligne[8].split(",");
			
			for (String tag : tags) {
				try {
					context.write(new Text(Country.getCountryAt(Double.parseDouble(ligne[11]), Double.parseDouble(ligne[10])).toString()), new Text(tag));
					// output mapper : < IdPays , Tag >
					
				} catch (Exception e) {
					// ignore malformated line
				}

			}

		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// input reducer : < IdPays , Iterable<Tag> >

			Map<String, Integer> map = new HashMap<>();

			for (Text tag : values) {
				if (map.get(tag.toString()) != null) {
					map.replace(tag.toString(), map.get(tag.toString())+1);
				}else{
					map.put(tag.toString(), 1);
				}
					
			}
			
			for (Map.Entry<String, Integer> entry : map.entrySet()) {
					context.write(new Text(key + " "+ entry.getKey()), new IntWritable(entry.getValue()));
						
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question2_1_1");
		job.setJarByClass(Question2_1_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}