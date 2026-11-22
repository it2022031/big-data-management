package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;          // ΑΛΛΑΓΗ: χρειάζεται για το όνομα του αρχείου
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Hello {

    // MAP: για κάθε λέξη της γραμμής βγάζει (λέξη, "αρχείο:1")
    public static class HelloMapper extends Mapper<LongWritable, Text, Text, Text> {   // ΑΛΛΑΓΗ: value Text αντί για IntWritable

        private final Text outValue = new Text();                                       // ΑΛΛΑΓΗ: κρατάει το "zoo:1"

        @Override
        protected void setup(Context context) {                                         // ΑΛΛΑΓΗ: νέα μέθοδος
            String name = ((FileSplit) context.getInputSplit()).getPath().getName();    // Παίρνει το όνομα του αρχείου, π.χ. "zoo.txt"
            String file = name.replace(".txt", "");                                     // Αφαιρεί την κατάληξη: "zoo"
            outValue.set(file + ":1");                                                  // Ετοιμάζει το value: "zoo:1"
        }

        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            for (String w : line.toString().split(" ")) {
                context.write(new Text(w), outValue);                                   // ΑΛΛΑΓΗ: γράφει (λέξη, "zoo:1")
            }
        }
    }

    // REDUCE: παίρνει (λέξη, ["zoo:1","farm:1",...]) και τα γράφει όλα σε μία γραμμή
    public static class HelloReducer extends Reducer<Text, Text, Text, Text> {         // ΑΛΛΑΓΗ: values Text
        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String all = "";
            for (Text v : values) {
                if (!all.isEmpty()) all += ", ";
                all += v.toString();                                                    // ΑΛΛΑΓΗ: ενώνει τα values αντί να αθροίζει
            }
            context.write(word, new Text(all));
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "hello");
        job.setJarByClass(Hello.class);
        job.setMapperClass(HelloMapper.class);
        job.setReducerClass(HelloReducer.class);
        job.setOutputKeyClass(Text.class);         // ΑΛΛΑΓΗ: Text αντί για IntWritable
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}