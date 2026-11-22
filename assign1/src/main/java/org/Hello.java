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
import java.util.Map;
import java.util.TreeMap;

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
                context.write(new Text("*"), outValue);                                 // Γράφει ("*", "zoo:1") για το σύνολο λέξεων του αρχείου
            }
        }
    }

    // REDUCE: για το "*" κρατάει τα σύνολα ανά αρχείο, για κάθε λέξη γράφει πλήθος / σύνολο αρχείου
    // Προϋπόθεση: 1 reducer, ώστε το "*" (που ταξινομείται πρώτο) να φτάνει πριν από τις λέξεις
    public static class HelloReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Integer> totals = new TreeMap<>();                         // Κρατάει τα σύνολα λέξεων ανά αρχείο (από το "*")

        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // Κρατάει άθροισμα ανά αρχείο, ταξινομημένο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // "zoo:1" → ["zoo", "1"]
                String file = parts[0];                                                 // "zoo"
                int n = Integer.parseInt(parts[1]);                                     // 1
                counts.merge(file, n, Integer::sum);                                    // Προσθέτει στο άθροισμα του αρχείου
            }

            if (word.toString().equals("*")) {                                          // Το "*" έρχεται πρώτο λόγω ταξινόμησης
                totals = counts;                                                        // Αποθηκεύει τα σύνολα, π.χ. {farm=4, zoo=6}
                                                             // Δεν γράφει γραμμή για το "*"
            }

            for (Map.Entry<String, Integer> e : counts.entrySet()) {                    // Γράφει μία γραμμή για κάθε αρχείο
                double ratio = (double) e.getValue() / totals.get(e.getKey());          // Πλήθος λέξης / σύνολο λέξεων του αρχείου
                context.write(word, new Text(e.getKey() + "\t" + e.getValue() + "\t" + ratio));
            }
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "hello");
        job.setJarByClass(Hello.class);
        job.setMapperClass(HelloMapper.class);
        job.setReducerClass(HelloReducer.class);
        job.setNumReduceTasks(1);                                                       // Ένας reducer: απαιτείται για να βλέπει το "*" πριν από τις λέξεις
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}