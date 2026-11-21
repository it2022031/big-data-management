package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Hello {

    // MAP: για κάθε λέξη της γραμμής βγάζει (λέξη, 1)
    public static class HelloMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            for (String w : line.toString().split(" ")) {
                context.write(new Text(w), new IntWritable(1));
            }
        }
    }

    // REDUCE: παίρνει (λέξη, [1,1,1]) και βγάζει (λέξη, 3)
    public static class HelloReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> ones, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable one : ones) sum += one.get();
            context.write(word, new IntWritable(sum));
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "hello");
        job.setJarByClass(Hello.class);
        job.setMapperClass(HelloMapper.class);
        job.setReducerClass(HelloReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}