import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

public class Indexing {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(Indexing.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompositeWritable.class);
        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    private static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            final String[] words = value.toString().split("\\s+");
            for (String word : words) {
                word = word.toLowerCase();
                word = word.replaceAll("[\\W|\\d]", "");
                if (word.length() > 0) {
                    context.write(new Text(word), new Text(fs.getPath().getName()));
                }
            }
        }
    }

    private static class Reduce extends Reducer<Text, Text, Text, CompositeWritable> {
        @Override
        protected void reduce(Text word, Iterable<Text> fileNames, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> counts = new HashMap<String, Integer>();
            for (Text fileName : fileNames) {
                if (counts.containsKey(fileName.toString())) {
                    Integer count = counts.get(fileName.toString());
                    counts.put(fileName.toString(), count + 1);
                } else {
                    counts.put(fileName.toString(), 1);
                }
            }

            Set<String> fileNamesSet = counts.keySet();
            for (String fileName : fileNamesSet) {
                context.write(word, new CompositeWritable(fileName, counts.get(fileName)));
            }
        }
    }

}
