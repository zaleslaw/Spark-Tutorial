import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class ImprovedWordCount extends Configured implements Tool {
    private static Set<String> stopwords;

    static {
        stopwords = new HashSet<String>();
        stopwords.add("I"); stopwords.add("a");
        stopwords.add("about"); stopwords.add("an");
        stopwords.add("are"); stopwords.add("as");
        stopwords.add("at"); stopwords.add("be");
        stopwords.add("by"); stopwords.add("com");
        stopwords.add("de"); stopwords.add("en");
        stopwords.add("for"); stopwords.add("from");
        stopwords.add("how"); stopwords.add("in");
        stopwords.add("is"); stopwords.add("it");
        stopwords.add("la"); stopwords.add("of");
        stopwords.add("on"); stopwords.add("or");
        stopwords.add("that"); stopwords.add("the");
        stopwords.add("this"); stopwords.add("to");
        stopwords.add("was"); stopwords.add("what");
        stopwords.add("when"); stopwords.add("where");
        stopwords.add("who"); stopwords.add("will");
        stopwords.add("with"); stopwords.add("and");
        stopwords.add("the"); stopwords.add("www");
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf()); // new approach
        // to take on fly properties and configurations
        job.setJobName("myjob");
        /*
        To distribute code across nodes (not main class only)
         */
        job.setJarByClass(ImprovedWordCount.class); // fix ClassNotFoundException (Map or Reduce class)

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Reduce.class); // add combiner
        job.setNumReduceTasks(4);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken().toLowerCase());//make lower case
                if(stopwords.contains(word.toString())){ // add filters
                    continue;
                }
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }



    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImprovedWordCount(), args);
        System.exit(res);
    }

}