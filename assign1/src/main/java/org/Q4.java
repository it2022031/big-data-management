package org;

/*
 * Big Data Management 2026-2027 - Εργασία 1 - Ερώτημα 4 (Σημειακή αμοιβαία πληροφορία (PMI))
 * Ομάδα 8: Αίας Κωνσταντίνος Καρκάνης (ais26108), Σταυρούλα Πάρσαλη (ais26115)
 */

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Q4 {

    private static final int FIRST_WORDS    = 40;   // Πόσες λέξεις κρατάει από την αρχή κάθε γραμμής
    private static final int MIN_LENGTH     = 4;    // Ελάχιστο μήκος λέξης
    private static final int MIN_PAIR_COUNT = 10;   // Ελάχιστο count(x,y) για να γραφτεί ένα ζεύγος

    // Σπάει μια γραμμή σε λέξεις: πεζά γράμματα, λέξη = μέγιστη ακολουθία a-z
    private static List<String> tokenize(String line) {
        List<String> words = new ArrayList<>();
        for (String w : line.toLowerCase().split("[^a-z]+")) {          // Μετατρέπει σε πεζά και κόβει σε κάθε μη-γράμμα
            if (!w.isEmpty()) {                                         // Παραλείπει τα κενά κομμάτια
                words.add(w);
            }
        }
        return words;
    }

    // Φτιάχνει το σύνολο λέξεων μιας γραμμής: πρώτες 40 λέξεις → μόνο ≥4 χαρακτήρων → διακριτές, ταξινομημένες
    private static Set<String> lineWords(String line) {
        List<String> tokens = tokenize(line);                           // 1. Πεζά, λέξεις a-z
        if (tokens.size() > FIRST_WORDS) {
            tokens = tokens.subList(0, FIRST_WORDS);                    // 2. Κρατάει μόνο τις πρώτες 40
        }
        Set<String> words = new TreeSet<>();                            // 4. Σύνολο: κάθε λέξη μία φορά, ταξινομημένο
        for (String w : tokens) {
            if (w.length() >= MIN_LENGTH) {                             // 3. Μόνο λέξεις με ≥4 χαρακτήρες
                words.add(w);
            }
        }
        return words;
    }

    // =====================================================================
    // JOB 1: count(x) και N, ανά αρχείο
    // =====================================================================

    // MAP: ανά γραμμή ("*", "pos:1") για το N, και (λέξη, "pos:1") για κάθε λέξη του συνόλου της γραμμής
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String file;                                            // "pos", "neg" ή "sotu"
        private final Text outValue = new Text();                       // Κρατάει το "pos:1" ή "neg:1"

        @Override
        protected void setup(Context context) {
            String name = ((FileSplit) context.getInputSplit()).getPath().getName();   // Παίρνει το όνομα του αρχείου, π.χ. "pos.txt"
            file = name.replace(".txt", "");                                           // Αφαιρεί την κατάληξη: "pos"
            outValue.set(file + ":1");                                                 // Ετοιμάζει το value: "pos:1"
        }

        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {

            if (!file.equals("pos") && !file.equals("neg")) {          // Το Q4 αφορά μόνο pos και neg: το sotu αγνοείται
                return;
            }

            context.write(new Text("*"), outValue);                     // Μία φορά ανά γραμμή, ακόμα και κενή → N

            for (String w : lineWords(line.toString())) {               // Κάθε λέξη μία φορά ανά γραμμή (Set)
                context.write(new Text(w), outValue);                   // Γράφει (λέξη, "pos:1") → count(x)
            }
        }
    }

    // COMBINE: τρέχει σε κάθε map task πριν από το shuffle, αθροίζει τα values ανά αρχείο
    // Γράφει στην ΙΔΙΑ μορφή με τον mapper ("pos:3"), ώστε ο reducer να τα διαβάζει κανονικά
    public static class SumCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // Κρατάει άθροισμα ανά αρχείο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // "pos:1" → ["pos", "1"]
                String file = parts[0];                                                 // "pos"
                int n = Integer.parseInt(parts[1]);                                     // 1
                counts.merge(file, n, Integer::sum);                                    // Προσθέτει στο άθροισμα του αρχείου
            }

            for (Map.Entry<String, Integer> e : counts.entrySet()) {                    // Γράφει ένα ζεύγος ανά αρχείο
                context.write(key, new Text(e.getKey() + ":" + e.getValue()));          // π.χ. (morning, "pos:500")
            }
        }
    }

    // REDUCE: αθροίζει ανά αρχείο και γράφει "λέξη  αρχείο  πλήθος"
    // Παραλείπει λέξεις με count < 10: δεν μπορούν να ανήκουν σε ζεύγος με count(x,y) ≥ 10
    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // Κρατάει άθροισμα ανά αρχείο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // "pos:500" → ["pos", "500"]
                String file = parts[0];                                                 // "pos"
                int n = Integer.parseInt(parts[1]);                                     // 500
                counts.merge(file, n, Integer::sum);                                    // Προσθέτει στο άθροισμα του αρχείου
            }

            boolean isTotal = key.toString().equals("*");                               // Το "*" κρατάει το N

            for (Map.Entry<String, Integer> e : counts.entrySet()) {                    // Μία γραμμή ανά αρχείο
                if (isTotal || e.getValue() >= MIN_PAIR_COUNT) {                         // Το N γράφεται πάντα, οι λέξεις μόνο αν count ≥ 10
                    context.write(key, new Text(e.getKey() + "\t" + e.getValue()));     // π.χ. "morning  pos  500" ή "*  pos  32937"
                }
            }
        }
    }

    // DRIVER (προσωρινά τρέχει μόνο το Job 1)
    public static void main(String[] args) throws Exception {
        Job job1 = Job.getInstance(new Configuration(), "q4-counts");
        job1.setJarByClass(Q4.class);
        job1.setMapperClass(CountMapper.class);
        job1.setCombinerClass(SumCombiner.class);                                       // Αθροίζει τοπικά σε κάθε map task πριν από το shuffle
        job1.setReducerClass(CountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}