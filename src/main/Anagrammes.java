package main;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Anagrammes {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private Text sortedText = new Text();
		private Text orginalText = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, Text> outputCollector, Reporter reporter)
				throws IOException {

			String word = value.toString();
			char[] wordChars = word.toCharArray();
			Arrays.sort(wordChars);
			String sortedWord = new String(wordChars);
			sortedText.set(sortedWord);
			orginalText.set(word);
			outputCollector.collect(sortedText, orginalText);
		}

	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void reduce(Text anagramKey, Iterator<Text> anagramValues,
				OutputCollector<Text, Text> results, Reporter reporter)
				throws IOException {
			
			String output = "";
			while (anagramValues.hasNext()) {
				Text anagam = anagramValues.next();
				output = output + anagam.toString() + "~";
			}
			StringTokenizer outputTokenizer = new StringTokenizer(output, "~");
			if (outputTokenizer.countTokens() >= 2) {
				output = output.replace("~", ",");
				outputKey.set(anagramKey.toString());
				outputValue.set(output);
				results.collect(outputKey, outputValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "anagramme");
		job.setJarByClass(Anagrammes.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}