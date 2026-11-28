package org;

/*
 * Big Data Management 2026-2027 - Εργασία 1 - Ερώτημα 2 (Διακριτικές λέξεις)
 * Ομάδα: <Ονοματεπώνυμο 1> (<ΑΜ 1>), <Ονοματεπώνυμο 2> (<ΑΜ 2>)
 *
 * Ένα job:
 *   MAP      : (λέξη, "pos:1") για κάθε λέξη ≥3 χαρακτήρων και ("*", "pos:<λέξεις γραμμής>") για το Npos/Nneg
 *   COMBINE  : αθροίζει ανά αρχείο → (λέξη, "pos:<άθροισμα>")
 *   REDUCE   : 1 reducer. Το "*" ταξινομείται πρώτο, οπότε τα Npos/Nneg είναι γνωστά πριν από τις λέξεις.
 *              Για κάθε λέξη με cpos ≥ 50 και cneg ≥ 50 υπολογίζει το r και το κρατάει σε λίστα.
 *   CLEANUP  : ταξινομεί κατά r (φθίνον, σε ισοβαθμία αλφαβητικά) και γράφει τις 20 πρώτες και τις 20 τελευταίες.
 *
 * Εκτέλεση: hadoop jar <jar> org.Q2 /user/root/input /user/root/q2_out
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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Q2 {

    private static final int MIN_LENGTH = 3;     // Ελάχιστο μήκος λέξης για να υπολογιστεί r
    private static final int MIN_COUNT  = 50;    // Ελάχιστες εμφανίσεις σε pos ΚΑΙ σε neg
    private static final int TOP        = 20;    // Πόσες λέξεις γράφονται από την κάθε άκρη

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

    // MAP: για κάθε λέξη ≥3 χαρακτήρων βγάζει (λέξη, "pos:1"), και ανά γραμμή ("*", "pos:<πλήθος λέξεων>")
    public static class Q2Mapper extends Mapper<LongWritable, Text, Text, Text> {

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

            if (!file.equals("pos") && !file.equals("neg")) {          // Το Q2 αφορά μόνο pos και neg: το sotu αγνοείται
                return;
            }

            List<String> words = tokenize(line.toString());

            for (String w : words) {
                if (w.length() >= MIN_LENGTH) {                         // Οι μικρές λέξεις δεν παίρνουν ποτέ r, οπότε δεν στέλνονται
                    context.write(new Text(w), outValue);               // Γράφει (λέξη, "pos:1")
                }
            }

            if (!words.isEmpty()) {                                     // Το Npos/Nneg μετράει ΟΛΕΣ τις λέξεις, κάθε μήκους
                context.write(new Text("*"), new Text(file + ":" + words.size()));   // Γράφει ("*", "pos:<λέξεις γραμμής>")
            }
        }
    }

    // COMBINE: τρέχει σε κάθε map task πριν από το shuffle, αθροίζει τα values ανά αρχείο
    // Γράφει στην ΙΔΙΑ μορφή με τον mapper ("pos:3"), ώστε ο reducer να τα διαβάζει κανονικά
    public static class Q2Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // Κρατάει άθροισμα ανά αρχείο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // "pos:1" → ["pos", "1"]
                counts.merge(parts[0], Integer.parseInt(parts[1]), Integer::sum);       // Προσθέτει στο άθροισμα του αρχείου
            }

            for (Map.Entry<String, Integer> e : counts.entrySet()) {                    // Γράφει ένα ζεύγος ανά αρχείο
                context.write(word, new Text(e.getKey() + ":" + e.getValue()));         // π.χ. (film, "pos:3")
            }
        }
    }

    // REDUCE: για το "*" κρατάει τα Npos/Nneg, για κάθε λέξη υπολογίζει το r, και στο cleanup() γράφει τα 20+20
    // Προϋπόθεση: 1 reducer, ώστε το "*" (που ταξινομείται πρώτο) να φτάνει πριν από τις λέξεις
    public static class Q2Reducer extends Reducer<Text, Text, Text, Text> {

        private Map<String, Integer> totals = new TreeMap<>();                         // Κρατάει τα Npos, Nneg (από το "*")
        private final List<Map.Entry<String, Double>> results = new ArrayList<>();     // Κρατάει (λέξη, r) από όλες τις κλήσεις, για το cleanup()

        @Override
        protected void reduce(Text word, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Integer> counts = new TreeMap<>();                              // Κρατάει άθροισμα ανά αρχείο

            for (Text v : values) {
                String[] parts = v.toString().split(":");                               // "pos:3" → ["pos", "3"]
                String file = parts[0];                                                 // "pos"
                int n = Integer.parseInt(parts[1]);                                     // 3
                counts.merge(file, n, Integer::sum);                                    // Προσθέτει στο άθροισμα του αρχείου
            }

            if (word.toString().equals("*")) {                                          // Το "*" έρχεται πρώτο λόγω ταξινόμησης
                totals = counts;                                                        // Αποθηκεύει τα σύνολα, π.χ. {neg=..., pos=...}
                return;                                                                 // Δεν γράφει γραμμή για το "*"
            }

            int cpos = counts.getOrDefault("pos", 0);                                   // Εμφανίσεις στο pos (0 αν δεν υπάρχει)
            int cneg = counts.getOrDefault("neg", 0);                                   // Εμφανίσεις στο neg (0 αν δεν υπάρχει)

            if (cpos >= MIN_COUNT && cneg >= MIN_COUNT) {                               // Φίλτρο: τουλάχιστον 50 φορές ΚΑΙ στα δύο αρχεία
                double rPos = (double) cpos / totals.get("pos");                        // Συχνότητα στο pos: cpos / Npos
                double rNeg = (double) cneg / totals.get("neg");                        // Συχνότητα στο neg: cneg / Nneg
                results.add(new AbstractMap.SimpleEntry<>(word.toString(), rPos / rNeg));  // Κρατάει (λέξη, r) για το τέλος, χωρίς να γράφει
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            results.sort((a, b) -> {                                                    // Ταξινομεί όλα τα (λέξη, r)
                int c = Double.compare(b.getValue(), a.getValue());                     // Πρώτα κατά r, από το μεγαλύτερο στο μικρότερο
                return (c != 0) ? c : a.getKey().compareTo(b.getKey());                 // Σε ισοβαθμία, αλφαβητικά
            });

            for (int i = 0; i < results.size(); i++) {
                if (i < TOP || i >= results.size() - TOP) {                             // Μόνο οι 20 πρώτες και οι 20 τελευταίες θέσεις
                    Map.Entry<String, Double> e = results.get(i);
                    context.write(new Text(e.getKey()), new Text(String.format("%.3f", e.getValue())));
                }
            }
        }
    }

    // DRIVER
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "q2");
        job.setJarByClass(Q2.class);
        job.setMapperClass(Q2Mapper.class);
        job.setCombinerClass(Q2Combiner.class);                                         // Αθροίζει τοπικά σε κάθε map task πριν από το shuffle
        job.setReducerClass(Q2Reducer.class);
        job.setNumReduceTasks(1);                                                       // Ένας reducer: απαιτείται για το "*" και για το καθολικό top/bottom 20
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}