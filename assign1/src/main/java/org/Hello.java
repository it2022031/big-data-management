package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Hello {

    // JOB 1 - MAP: (λέξη, 1)
    public static class HelloMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            for (String w : line.toString().split(" ")) {
                context.write(new Text(w), new IntWritable(1));
            }
        }
    }

    // JOB 1 - REDUCE: (λέξη, άθροισμα)
    public static class HelloReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> ones, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable one : ones) sum += one.get();
            context.write(word, new IntWritable(sum));
        }
    }

    // JOB 2 - MAP: για κάθε λέξη γράφει το πλήθος της, που το παίρνει από το αρχείο του Job 1
    public static class CacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Map<String, Integer> counts = new HashMap<>();   // Κρατάει το αρχείο του Job 1 στη μνήμη

        @Override
        protected void setup(Context context) throws IOException {
            for (URI uri : context.getCacheFiles()) {                    // Τα αρχεία που δηλώθηκαν με addCacheFile
                String name = new Path(uri.getPath()).getName();         // π.χ. "part-r-00000"
                BufferedReader br = new BufferedReader(new FileReader(name));
                String l;
                while ((l = br.readLine()) != null) {                    // Κάθε γραμμή: "cat\t4"
                    String[] parts = l.split("\t");
                    counts.put(parts[0], Integer.parseInt(parts[1]));    // cat → 4
                }
                br.close();
            }
        }

        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            for (String w : line.toString().split(" ")) {
                context.write(new Text(w), new IntWritable(counts.get(w)));   // Γράφει (λέξη, πλήθος από το Job 1)
            }
        }
    }

    // DRIVER: Job 1 → περιμένει → Job 2
    public static void main(String[] args) throws Exception {

        // ---- Job 1 ----
        Job job1 = Job.getInstance(new Configuration(), "job1");
        job1.setJarByClass(Hello.class);
        job1.setMapperClass(HelloMapper.class);
        job1.setReducerClass(HelloReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "_job1"));
        job1.waitForCompletion(true);                                    // Περιμένει να τελειώσει το Job 1

        // ---- Job 2 ----
        Job job2 = Job.getInstance(new Configuration(), "job2");
        job2.setJarByClass(Hello.class);
        job2.setMapperClass(CacheMapper.class);
        job2.setNumReduceTasks(0);                                       // Χωρίς reducer
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.addCacheFile(new Path(args[1] + "_job1/part-r-00000").toUri());   // Το αρχείο του Job 1 ως side data
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "_job2"));
        job2.waitForCompletion(true);
    }
}