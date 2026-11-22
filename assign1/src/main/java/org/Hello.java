package org;

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
import java.util.Map;                                                                   // ΑΛΛΑΓΗ
import java.util.TreeMap;                                                               // ΑΛΛΑΓΗ

public class Hello {

    // MAP: για κάθε λέξη της γραμμής βγάζει (λέξη, "αρχείο:1")
    public static class HelloMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text outValue = new Text();                                       // Κρατάει το "zoo:1"

        @Override
        protected void setup(Context context) {
            String name = ((FileSplit) context.getInputSplit()).getPath().getName();    // Παίρνει το όνομα του αρχείου, π.χ. "zoo.txt"
            String file = name.replace(".txt", "");                                     // Αφαιρεί την κατάληξη: "zoo"
            outValue.set(file + ":1");                                                  // Ετοιμάζει το value: "zoo:1"
        }

        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {
            for (String w : line.toString().split(" ")) {
                context.write(new Text(w), outValue);                                   // Γράφει (λέξη, "zoo:1")
            }
        }
    }

    // REDUCE: παίρνει (λέξη, ["zoo:1","zoo:1","farm:1"]) και γράφει μία γραμμή ανά αρχείο
    public static class HelloReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // ΑΛΛΑΓΗ: Κρατάει άθροισμα ανά αρχείο, ταξινομημένο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // ΑΛΛΑΓΗ: "zoo:1" → ["zoo", "1"]
                String file = parts[0];                                                 // ΑΛΛΑΓΗ: "zoo"
                int n = Integer.parseInt(parts[1]);                                     // ΑΛΛΑΓΗ: 1
                counts.merge(file, n, Integer::sum);                                    // ΑΛΛΑΓΗ: Προσθέτει στο άθροισμα του αρχείου
            }

            for (Map.Entry<String, Integer> e : counts.entrySet()) {                    // ΑΛΛΑΓΗ: Γράφει μία γραμμή για κάθε αρχείο
                context.write(word, new Text(e.getKey() + "\t" + e.getValue()));
            }
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "hello");
        job.setJarByClass(Hello.class);
        job.setMapperClass(HelloMapper.class);
        job.setReducerClass(HelloReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}