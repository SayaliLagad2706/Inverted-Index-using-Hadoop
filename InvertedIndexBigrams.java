import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
		if(args.length != 2) {
			System.err.println("Usage: Word Count <input path> <output path>");
			System.exit(-1);
		}

		@SuppressWarnings({ "deprecation" })
		Job job = new Job();
		job.setJarByClass(WordCount.class);
		job.setJobName("Word Count");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}

}

class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text documentID;
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String word1 = "", word2 = "";
		String[] str = value.toString().split("\\t");
		documentID = new Text(str[0]);
		String line = str[1];

		// replace all non-alphabetical characters with a space
		line = line.replaceAll("[^a-zA-Z]", " ");
		line = line.toLowerCase();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		if(tokenizer.hasMoreTokens()) {
			word1 = tokenizer.nextToken();
		}
		
		while(tokenizer.hasMoreTokens()) {
			word2 = tokenizer.nextToken();
			word.set(word1 + " " + word2);
			context.write(word, documentID);
			word1 = word2;
		}
	}
}

class WordCountReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		
		for(Text value: values) {
			map.put(value.toString(), map.getOrDefault(value.toString(), 0)+1);
		}
		
		String val = map.toString().replaceAll(",", "").replaceAll("\\{", "").replaceAll("\\}", "").replaceAll("=", ":");
		context.write(key, new Text(val));
	}
}
