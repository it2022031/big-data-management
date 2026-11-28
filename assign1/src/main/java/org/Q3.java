package org;

/*
 * Big Data Management 2026-2027 - Εργασία 1 - Ερώτημα 3 (Bigrams και side data)
 * Ομάδα 8: Αίας Κωνσταντίνος Καρκάνης (ais26108), Σταυρούλα Πάρσαλη (ais26115)
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q3 {

    //σταθερα για το πληθος των κορυφαιων bigrams που κραταμε ανα αρχειο
    private static final int TOP_K = 10;

    //βοηθητικη κλαση που αναπαριστα ενα ζευγος (bigram, count)
    private static class BigramEntry {
        final String bigram;
        final int count;

        BigramEntry(String bigram, int count) {
            this.bigram = bigram;
            this.count = count;
        }
    }

    //δομη επιλογης top-k με χρηση Min-Heap (PriorityQueue)
    //κραταμε στην μνημη μονο τα Κ καλυτερα στοιχεια αντι να ταξινομουμε ολα τα bigrams
    private static class TopKHeap {
        //min-heap: στην κορυφη του σωρου βρισκεται παντα το "χειροτερο" απο τα k στοιχεια
        //1ο κριτηριο: μικροτερη συχνοτητα (a.count vs b.count)
        //2o κριτηριο (σε ισοβαθμια): το αλφαβητικα μεγαλυτερο bigram (b vs a), ωστε να απορριπτεται πρωτο
        private final PriorityQueue<BigramEntry> heap = new PriorityQueue<>((a, b) -> {
            int freqCmp = Integer.compare(a.count, b.count);
            if (freqCmp != 0) {
                return freqCmp;
            }
            return b.bigram.compareTo(a.bigram);
        });

        //εισαγει νεο bigram και αν το heap ξεπερασει το TOP_K , πεταει την κορυφη (το χειροτερο)
        void add(String bigram, int count) {
            heap.offer(new BigramEntry(bigram, count));
            if (heap.size() > TOP_K) {
                heap.poll();
            }
        }

        //εξαγει τα στοιχεια του σωρου σε sorted λιστα για την τελικη εκτυπωση
        //φθινουσα συχνοτητα και σε ισοβαθμια αλφαβητικα αυξουσα
        List<BigramEntry> getSortedDescending() {
            List<BigramEntry> list = new ArrayList<>(heap);
            list.sort((a, b) -> {
                int freqCmp = Integer.compare(b.count, a.count);
                if (freqCmp != 0) {
                    return freqCmp;
                }
                return a.bigram.compareTo(b.bigram);
            });
            return list;
        }
    }

    //------
    //JOB 1: καταμετρηση εγκυρων bigrams ανα αρχειο (με distributed cache)
    //------

    //MAPPER 1
    //τυποι <K1,V1,K2,V2>:
    //input: K1 (byte offset της γραμμης), V1 (κειμενο της γραμμης)\
    //output: K2 ("αρχειο\tword1 word2"), V2 (1)
    public static class BigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        //μεταγλωτιζουμε το regex [a-z]+ μια φορα για ολη τη κλαση
        private static final Pattern WORD_PATTERN = Pattern.compile("[a-z]+");
        //σταθερο αντικειμενο για να μην δημιουργειται νεο σε καθε bigram
        private static final IntWritable ONE = new IntWritable(1);

        //HashSet για αποθηκευση των stop words στη RAM -> ελεγχος contains() σε μεσο χρονο O(1)
        private final Set<String> stopwords = new HashSet<>();
        private final Text outKey = new Text(); //περιορισμος του garbage collection
        private String docName; //ονομα του αρχειου εισοδου

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //ανακτηση ονοματος αρχειου απο το FileSplit χωρις το πληρες HDFS path
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.endsWith(".txt")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            docName = fileName;

            //αναγνωση της λιστας stop words απο το symlink "stopwords" του distributed cache
            try (BufferedReader reader = new BufferedReader(new FileReader("stopwords"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String word = line.trim().toLowerCase(Locale.ROOT);
                    if (!word.isEmpty()) {
                        stopwords.add(word);
                    }
                }
            }
        }

        //η map() καλειται μια φορα για καθε γραμμη του FileSplit
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //μετατροπη γραμμης σε πεζα και σαρωση με το WORD_PATTERN
            String line = value.toString().toLowerCase(Locale.ROOT);
            Matcher matcher = WORD_PATTERN.matcher(line);

            //κραταμε την αμεσως προηγουμενη λεξη της ιδιας γραμμης και αν ηταν stop word
            String prevWord = null;
            boolean prevIsStop = false;

            while (matcher.find()) {
                String currWord = matcher.group(); //τρεχουσα λεξη
                boolean currIsStop = stopwords.contains(currWord); //ελεγχος αν ειναι stop word

                //εκπεμπουμε το bigram ΜΟΝΟ αν υπαρχει προηγουμενη λεξη στην ιδια γραμμη
                //και καμια απο τις δυο διαδοχικες λεξεις δεν ειναι stop word
                if (prevWord != null && !prevIsStop && !currIsStop) {
                    outKey.set(docName + "\t" + prevWord + " " + currWord);
                    context.write(outKey, ONE);
                }

                //ενημερωση της προηγουμενης λεξης για το επομενο ζευγος της γραμμης
                prevWord = currWord;
                prevIsStop = currIsStop;
            }
        }
    }

    //COMBINER/REDUCER 1
    //τυποι <K2,V2,K2,V2>:
    //εισοδος: K2 ("doc\tw1 w2"), V2 (λιστα απο 1, ή μερικα αθροισματα στον Reducer)
    //εξοδος: K2 ("doc\tw1 w2"), V2 (συνολικη συχνοτητα του bigram στο συγκεκριμενο αρχειο)
    public static class BigramSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        //αθροιζει τις εμφανισεις καθε (doc, bigram) τοπικα (ως combiner) και συνολικα (ως reducer)
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    //------
    //JOB 2: επιλογη Top-10 bigrams ανα αρχειο
    //------

    //MAPPER 2
    //τυποι <K1,V1,K2,V2>:
    //εισοδος: K1 (byte offset), V1 ("doc\tbigram\tcount" απο την εξοδο του job 1)
    //εξοδος (στη cleanup): K2 (doc), V2 ("bigram\tcount") για τα τοπικα top-10 καθε αρχειου
    public static class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {

        //διατηρουμε ενα ξεχωριστο TopKHeap (μεγεθους 10) για καθε αρχειο
        private final Map<String, TopKHeap> localTop = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        //διαβαζει καθε γραμμη εξοδου του job 1 και ενημερωνει τον τοπικο σωρο του αντιστοιχου αρχειου
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 3) {
                return;
            }
            String doc = parts[0]; //ονομα αρχειο
            String bigram = parts[1]; //bigram (w1 w2)
            int count = Integer.parseInt(parts[2]); //συνολικη συχνοτητα

            TopKHeap heap = localTop.get(doc);
            if (heap == null) {
                heap = new TopKHeap();
                localTop.put(doc, heap);
            }
            heap.add(bigram, count);
        }

        //η cleanup() καλειται μια φορα στο τελος του map task
        //στελνει στο shuffle το πολυ 10 υποψηφια bigrams ανα αρχειο απο καθε mapper
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String doc : localTop.keySet()) {
                outKey.set(doc);
                for (BigramEntry entry : localTop.get(doc).getSortedDescending()) {
                    outVal.set(entry.bigram + "\t" + entry.count);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    //REDUCER 2
    //εισοδος: K2 (doc), V2 (λιστα υποψηφιων "bigram\tcount" απο ολους τους mappers)
    //εξοδος (στο HDFS): K3 (doc), V3 ("bigram\tcount") για τα τελικα top-10 του αρχειου
    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        //καλειται μια φορα για καθε αρχειο και βρισκει τα καθολικα top-10
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TopKHeap globalTop = new TopKHeap();

            for (Text val : values) {
                String s = val.toString();
                int sep = s.lastIndexOf('\t'); //διαχωρισμος bigram και συχνοτητας
                String bigram = s.substring(0, sep);
                int count = Integer.parseInt(s.substring(sep + 1));
                globalTop.add(bigram, count);
            }

            //εκτυπωνουμε τα 10 κορυφαια bigrams του αρχειου
            for (BigramEntry entry : globalTop.getSortedDescending()) {
                outVal.set(entry.bigram + "\t" + entry.count);
                context.write(key, outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //ελεγχος ορισματων: υποχρεωτικα input & output, προαιρετικα το HDFS path των stop words
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: Q3 <input path> <output path> [stopwords HDFS path]");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        //προσωρινος φακελος στο HDFS για την αποθηκευση της εξοδου του job 1
        Path tempPath = new Path(args[1] + "_tmp_counts");
        String stopwordsPath = (args.length == 3) ? args[2] : "/user/root/stopwords/english";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }


        //--- 1ο job: καταμετρηση bigrams ανα αρχειο ---
        Job job1 = Job.getInstance(conf, "Q3 Job 1 - Bigram Count");
        job1.setJarByClass(Q3.class);
        //δηλωση του αρχειου stop words στο distributed cache με local symlink "stopwords"
        job1.addCacheFile(new URI(stopwordsPath + "#stopwords"));

        job1.setMapperClass(BigramMapper.class);
        //χρηση combiner για τοπικη αθροιση και μειωση του shuffle
        job1.setCombinerClass(BigramSumReducer.class);
        job1.setReducerClass(BigramSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempPath);

        //εκτελεση του job 1, αν αποτυχει, τερματιζουμε αμεσα
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        //--- 2ο job: επιλογη top-10 bigrams ανα αρχειο ---
        Job job2 = Job.getInstance(conf, "Q3 Job 2 - Top-10 Bigrams per File");
        job2.setJarByClass(Q3.class);

        job2.setMapperClass(TopKMapper.class);
        job2.setReducerClass(TopKReducer.class);
        //1 reducer αρκει καθως υπαρχουν μονο 3 κλειδια (pos, neg, sotu) και το πολυ 30 εγγραφες
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //η εισοδος του job 2 ειναι η εξοδος του job 1
        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        boolean success = job2.waitForCompletion(true);

        //καθαρισμος του ενδιαμεσου προσωρινου φακελου απο το HDFS
        fs.delete(tempPath, true);

        System.exit(success ? 0 : 1);
    }
}