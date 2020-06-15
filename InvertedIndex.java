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
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}

}

class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private LongWritable documentID;
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String[] str = value.toString().split("\\t");
		documentID = new LongWritable(Long.parseLong(str[0]));
		String line = str[1];

		// replace all non-alphabetical characters with a space
		line = line.replaceAll("[^a-zA-Z]", " ");
		line = line.toLowerCase();
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		while(tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, documentID);
		}
	}
}

class WordCountReducer extends Reducer<Text, LongWritable, Text, Text> {
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		HashMap<LongWritable, Integer> map = new HashMap<LongWritable, Integer>();
		
		for(LongWritable value: values) {
			map.put(value, map.getOrDefault(value, 0)+1);
		}
		
		String val = map.toString().replaceAll(",", "").replaceAll("\\{", "").replaceAll("\\}", "").replaceAll("=", ":");
		context.write(key, new Text(val));
	}
}
