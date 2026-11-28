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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
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

    // COMBINE: τρέχει σε κάθε map task πριν από το shuffle, αθροίζει τα values ανά αρχείο
    // Γράφει στην ΙΔΙΑ μορφή με τον mapper ("zoo:3"), ώστε ο reducer να τα διαβάζει κανονικά
    public static class HelloCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // Κρατάει άθροισμα ανά αρχείο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // "zoo:1" → ["zoo", "1"]
                counts.merge(parts[0], Integer.parseInt(parts[1]), Integer::sum);       // Προσθέτει στο άθροισμα του αρχείου
            }

            for (Map.Entry<String, Integer> e : counts.entrySet()) {                    // Γράφει ένα ζεύγος ανά αρχείο
                context.write(word, new Text(e.getKey() + ":" + e.getValue()));         // π.χ. (cat, "zoo:3")
            }
        }
    }

    // REDUCE: για το "*" κρατάει τα σύνολα ανά αρχείο, για κάθε λέξη υπολογίζει το r, και στο cleanup() τα γράφει ταξινομημένα
    // Προϋπόθεση: 1 reducer, ώστε το "*" (που ταξινομείται πρώτο) να φτάνει πριν από τις λέξεις
    public static class HelloReducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Integer> totals = new TreeMap<>();                         // Κρατάει τα σύνολα λέξεων ανά αρχείο (από το "*")
        private final List<Map.Entry<String, Double>> results = new ArrayList<>();     // Κρατάει (λέξη, r) από όλες τις κλήσεις, για το cleanup()

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
                return;                                                                 // Δεν γράφει γραμμή για το "*"
            }

            if (counts.containsKey("farm") && counts.containsKey("zoo")) {              // Μόνο αν η λέξη υπάρχει και στα δύο αρχεία
                double rFarm = (double) counts.get("farm") / totals.get("farm");        // Συχνότητα στο farm, π.χ. 1 / 4
                double rZoo  = (double) counts.get("zoo")  / totals.get("zoo");         // Συχνότητα στο zoo, π.χ. 2 / 6
                results.add(new AbstractMap.SimpleEntry<>(word.toString(), rFarm / rZoo));  // Κρατάει (λέξη, r) για το τέλος, χωρίς να γράφει
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            results.sort((a, b) -> {                                                    // Ταξινομεί όλα τα (λέξη, r)
                int c = Double.compare(b.getValue(), a.getValue());                     // Πρώτα κατά r, από το μεγαλύτερο στο μικρότερο
                return (c != 0) ? c : a.getKey().compareTo(b.getKey());                 // Σε ισοβαθμία, αλφαβητικά
            });
            for (Map.Entry<String, Double> e : results) {                               // Γράφει τις γραμμές με τη σειρά ταξινόμησης
                context.write(new Text(e.getKey()), new Text(String.format("%.3f", e.getValue())));
            }
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "hello");
        job.setJarByClass(Hello.class);
        job.setMapperClass(HelloMapper.class);
        job.setCombinerClass(HelloCombiner.class);                                      // Αθροίζει τοπικά σε κάθε map task πριν από το shuffle
        job.setReducerClass(HelloReducer.class);
        job.setNumReduceTasks(1);                                                       // Ένας reducer: απαιτείται για να βλέπει το "*" πριν από τις λέξεις
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}