package org;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

    //MAPPER
    //τύποι <K1,V1,K2,V2>:
    //εισοδος: K1 (byte offset της γραμμης), V1 (κειμενο  της γραμμης)
    //εξοδος: K2 (λεξη), V2 ("αρχειο:1")
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

        //μεταγλωτιζουμε το regex [a-z]+ (καθε μεγιστη συνεχομενη ακολουθια πεζων αγγλικων γραμματων)
        //μια φορα για ολη την κλαση
        private static final Pattern WORD_PATTERN = Pattern.compile("[a-z]+");

        //επαναχρησιμοποιουμε τα ιδια 2 αντικειμενα Text σε καθε context.write()
        //για να μην δημιουργηθουν εκατομμυρια αντικειμενα και να μην επιβαρυνθει ο garbage collector
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        //το setup καλειται απο το hadoop μια φορα στην αρχη του map task
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();

            //εξαγουμε μονο το ονομα του αρχειου χωρις το πληρες HDFS path
            String fileName = fileSplit.getPath().getName();
            if (fileName.endsWith(".txt")) { //αφαιρεση του ".txt"
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            //αποθηκευουμε μονιμα στο outVal το π.χ. pos:1
            outVal.set(fileName + ":1");
        }

        //η map() καλειται αυτοματα μια φορα για καθε γραμμη του FileSplit
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //μετατροπη text->string και ολα τα γραμματα σε πεζα
            String line = value.toString().toLowerCase(Locale.ROOT);
            //matcher που θα σαρωσει τη γραμμη για το WORD_PATTERN
            Matcher matcher = WORD_PATTERN.matcher(line);

            //οσο υπαρχουν λεξεις στην γραμμη...
            while (matcher.find()) {
                String word = matcher.group(); //επιστρεφει το ακριβες substring που ταιριαξε (τη λεξη)
                if (word.length() >= 3) { //κραταμε λεξεις με >=3 χαρακτηρες
                    outKey.set(word);
                    //emit το intermediate record (word, "doc:1") στον buffer του map task
                    context.write(outKey, outVal);
                }
            }
        }
    }

    //COMBINER - mini reducer
    //καθε combiner επεκτεινει το reducer και πρεπει να εχει ιδιους τυπους εισοδου-εξοδου (K2,V2)->(K2,V2)
    //τρεχει τοπικα στον κομβο καθε mapper πριν μεταφερθουν τα δεδομενα στο δικτυο
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        //καλειται τοπικα για καθε μοναδικη λεξη (key) που παρηγαγε ο mapper, με ολα τα values της
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //map που αθροιξει τις εμφανισεις της λεξης ανα αρχειο
            Map<String, Integer> counts = new HashMap<>();

            //διατρεχουμε ολες τις τιμες
            for (Text val : values) {
                String s = val.toString(); // μετατροπη σε java string
                int sep = s.lastIndexOf(':'); //βρισκουμε τη θεση του ":"
                String doc = s.substring(0, sep); //τμημα αριστερα απο το ":" -> ονομα αρχειου
                int count = Integer.parseInt(s.substring(sep + 1)); //τμημα δεξια απο το ":"-> μετατροπη σε int -> συχνοτητα
                //προσθετουμε το count στο μεχρι τωρα αθροισμα αυτου του αρχειου
                counts.put(doc, counts.getOrDefault(doc, 0) + count);
            }

            //για καθε αρχειο που βρεθηκε στο hash map, κανουμε emit ενα ζευγος (word, "doc:sum")
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                outVal.set(entry.getKey() + ":" + entry.getValue());
                context.write(key, outVal);
            }
        }
    }

    //REDUCER
    //τυποι: <K2,V2,K3,V3>:
    //εισοδος (μετα απο shuffle kai sort): K2= λεξη, V2= τιμες "doc:count"
    //εξοδος (στο HDFS): Κ3=λεξη, V3="doc1:count1, doc2:count2,..."
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        //καλειται μια φορα για καθε μοναδικη λεξη σε ολοκληρο το dataset
        // με ολες τις τιμες της απο ολους τους mappers
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //hashmap που συσσωρευει το τελικο sum ανα αρχειο για τη συγκεκριμενη λεξη
            //αρχεια στα οποια η λεξη δεν εμφανιστηκε, παραλειπονται αυτοματα
            Map<String, Integer> counts = new HashMap<>();

            for (Text val : values) {
                String s = val.toString();
                int sep = s.lastIndexOf(':');
                String doc = s.substring(0, sep);
                int count = Integer.parseInt(s.substring(sep + 1));
                counts.put(doc, counts.getOrDefault(doc, 0) + count);
            }

            //φθινουσα σειρα συχνοτητας
            String[] docs = counts.keySet().toArray(new String[0]);
            Arrays.sort(docs, (a, b) -> {
                int freqCmp = Integer.compare(counts.get(b), counts.get(a));
                if (freqCmp != 0) {
                    return freqCmp;
                }
                return a.compareTo(b);
            });

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < docs.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                String doc = docs[i];
                sb.append(doc).append(":").append(counts.get(doc));
            }

            outVal.set(sb.toString());
            context.write(key, outVal);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: Q1 <input path> <output path> [no-combiner]");
            System.exit(1);
        }

        boolean useCombiner = !(args.length == 3 && args[2].equalsIgnoreCase("no-combiner"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, useCombiner ? "Q1 Inverted Index (with Combiner)" : "Q1 Inverted Index (no Combiner)");
        job.setJarByClass(Q1.class);

        job.setMapperClass(InvertedIndexMapper.class);
        if (useCombiner) {
            job.setCombinerClass(InvertedIndexCombiner.class);
        }
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            Counters counters = job.getCounters();
            System.out.println("========== Q1 COUNTERS (" + (useCombiner ? "WITH COMBINER" : "WITHOUT COMBINER") + ") ==========");
            System.out.println("Map output records     : " + counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
            System.out.println("Combine input records  : " + counters.findCounter(TaskCounter.COMBINE_INPUT_RECORDS).getValue());
            System.out.println("Combine output records : " + counters.findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS).getValue());
            System.out.println("Reduce shuffle bytes   : " + counters.findCounter(TaskCounter.REDUCE_SHUFFLE_BYTES).getValue());
            System.out.println("============================================================");
        }

        System.exit(success ? 0 : 1);
    }
}