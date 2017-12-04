
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Question2_2 {
	protected static final String K = "k";

	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt2> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] ligne = value.toString().split("\\t");
			if (!ligne[8].isEmpty() && !ligne[11].isEmpty() && !ligne[10].isEmpty()) {
				String[] tags = ligne[8].split(",");
				Country country = Country.getCountryAt(Double.parseDouble(ligne[11]), Double.parseDouble(ligne[10]));
				if (country != null) {
					for (String tag : tags) {
						context.write(new Text(country.toString()), new StringAndInt2(new Text(tag), 1));
					}
				}
			}

		}
	}

	public static class MyCombiner extends Reducer<Text, StringAndInt2, Text, StringAndInt2> {

		@Override
		protected void reduce(Text key, Iterable<StringAndInt2> value, Context context)
				throws IOException, InterruptedException {
			Map<String, StringAndInt2> map = new HashMap<>();
			PriorityQueue<StringAndInt2> priority = new PriorityQueue<>();
			int count;

			for (StringAndInt2 tag : value) {

				if (map.get(tag.getTag().toString()) != null) {
					StringAndInt2 s = map.get(tag.getTag().toString());
					count = map.get(tag.getTag().toString()).getNbrOcc();
					count++;
					map.replace(tag.getTag().toString(), s,
							new StringAndInt2(new Text(tag.getTag().toString()), count));
				} else {
					if (tag.getTag().toString() != "\\t") {
						map.put(tag.getTag().toString(), new StringAndInt2(new Text(tag.getTag()), tag.getNbrOcc()));

					}
				}
			}

			Iterator itr = map.keySet().iterator();
			while (itr.hasNext()) {
				String cle = (String) itr.next();
				StringAndInt2 val = (StringAndInt2) map.get(cle);
				System.out.println("cle : " + cle + " valeur : <" + val.getTag() + "," + val.getNbrOcc() + ">");
				priority.add(val);

			}

			int i = Integer.parseInt(context.getConfiguration().get(K));
			while (!priority.isEmpty() && i > 0) {
				StringAndInt2 s2;
				s2 = priority.remove();
				context.write(key, new StringAndInt2(new Text(s2.getTag()), s2.getNbrOcc()));
				i--;
			}

		}

	}

	public static class MyReducer extends Reducer<Text, StringAndInt2, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<StringAndInt2> values, Context context)
				throws IOException, InterruptedException {
			Map<String, StringAndInt2> map = new HashMap<>();
			PriorityQueue<StringAndInt2> priority = new PriorityQueue<>();
			int count;

			for (StringAndInt2 tag : values) {

				if (map.get(tag.getTag().toString()) != null) {
					count = map.get(tag.getTag().toString()).getNbrOcc();
					count++;
					System.out.println("verif in map before replace" + map.get(tag.getTag().toString()).getTag());
					map.put(tag.getTag().toString(), new StringAndInt2(new Text(tag.getTag().toString()), count));
					System.out.println("verif in map after replace " + map.get(tag.getTag().toString()).getTag());
				} else {
					if (tag.getTag().toString() != "\\t") {
						map.put(tag.getTag().toString(), new StringAndInt2(new Text(tag.getTag()), tag.getNbrOcc()));

					}
				}
			}

			Iterator itr = map.keySet().iterator();
			while (itr.hasNext()) {
				String cle = (String) itr.next();
				StringAndInt2 val = (StringAndInt2) map.get(cle);
				System.out.println("cle : " + cle + " valeur : <" + val.getTag() + "," + val.getNbrOcc() + ">");
				priority.add(val);

			}

			int i = Integer.parseInt(context.getConfiguration().get(K));
			while (!priority.isEmpty() && i > 0) {
				StringAndInt2 s2;
				s2 = priority.remove();
				System.out.println("itr " + i + " " + s2.getTag() + " " + s2.getNbrOcc());
				context.write(key, new Text(s2.getTag()));
				i--;
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		conf.set(K, otherArgs[2]);
		Job job = Job.getInstance(conf, "Question2_2");
		job.setJarByClass(Question2_2.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt2.class);

		job.setCombinerClass(MyCombiner.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}