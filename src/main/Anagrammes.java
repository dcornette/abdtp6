package main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Anagrammes {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

	}

	public static void main(String[] args) {

	}
}