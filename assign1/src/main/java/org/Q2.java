package org;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Q2 {

    // Οι δικοί μας counters: συνολικό πλήθος λέξεων (κάθε μήκους) σε pos και neg
    public enum WordCounters { POS_WORDS, NEG_WORDS }

    // Helper: lowercase + "λέξη" = μέγιστη ακολουθία a-z (ορισμός της εκφώνησης)
    private static List<String> tokenize(String line) {
        List<String> words = new ArrayList<>();
        for (String w : line.toLowerCase().split("[^a-z]+")) {
            if (!w.isEmpty()) {
                words.add(w);
            }
        }
        return words;
    }

    // =====================================================================
    // JOB 1 - MAPPER
    // input:  (θέση γραμμής, γραμμή)
    // output: (λέξη, "pos:1") ή (λέξη, "neg:1")
    // + counters POS_WORDS / NEG_WORDS για τα Npos, Nneg
    // =====================================================================
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String file;                       // "pos", "neg" ή "sotu"
        private final Text outKey = new Text();    // η λέξη
        private final Text outValue = new Text();  // "pos:1" ή "neg:1"

        @Override
        protected void setup(Context context) {
            // Ένα map task διαβάζει ΜΟΝΟ ένα αρχείο, οπότε το όνομα το βρίσκουμε μία φορά εδώ
            String name = ((FileSplit) context.getInputSplit()).getPath().getName();  // "pos.txt"
            file = name.replace(".txt", "");                                          // "pos"
            outValue.set(file + ":1");                                                // "pos:1"
        }

        @Override
        protected void map(LongWritable key, Text line, Context context)
                throws IOException, InterruptedException {

            // Το Q2 αφορά μόνο pos και neg: το sotu αγνοείται
            if (!file.equals("pos") && !file.equals("neg")) {
                return;
            }

            List<String> words = tokenize(line.toString());

            // Για κάθε λέξη: (λέξη, "pos:1")
            for (String w : words) {
                outKey.set(w);
                context.write(outKey, outValue);
            }

            // Npos / Nneg: ΟΛΕΣ οι λέξεις της γραμμής, κάθε μήκους
            if (file.equals("pos")) {
                context.getCounter(WordCounters.POS_WORDS).increment(words.size());
            } else {
                context.getCounter(WordCounters.NEG_WORDS).increment(words.size());
            }
        }
    }
}