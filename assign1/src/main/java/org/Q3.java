package org;

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

    private static final int TOP_K = 10;

    private static class BigramEntry {
        final String bigram;
        final int count;

        BigramEntry(String bigram, int count) {
            this.bigram = bigram;
            this.count = count;
        }
    }

    private static class TopKHeap {
        private final PriorityQueue<BigramEntry> heap = new PriorityQueue<>((a, b) -> {
            int freqCmp = Integer.compare(a.count, b.count);
            if (freqCmp != 0) {
                return freqCmp;
            }
            return b.bigram.compareTo(a.bigram);
        });

        void add(String bigram, int count) {
            heap.offer(new BigramEntry(bigram, count));
            if (heap.size() > TOP_K) {
                heap.poll();
            }
        }

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

    public static class BigramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final Pattern WORD_PATTERN = Pattern.compile("[a-z]+");
        private static final IntWritable ONE = new IntWritable(1);

        private final Set<String> stopwords = new HashSet<>();
        private final Text outKey = new Text();
        private String docName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if (fileName.endsWith(".txt")) {
                fileName = fileName.substring(0, fileName.length() - 4);
            }
            docName = fileName;

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

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase(Locale.ROOT);
            Matcher matcher = WORD_PATTERN.matcher(line);

            String prevWord = null;
            boolean prevIsStop = false;

            while (matcher.find()) {
                String currWord = matcher.group();
                boolean currIsStop = stopwords.contains(currWord);

                if (prevWord != null && !prevIsStop && !currIsStop) {
                    outKey.set(docName + "\t" + prevWord + " " + currWord);
                    context.write(outKey, ONE);
                }

                prevWord = currWord;
                prevIsStop = currIsStop;
            }
        }
    }

    public static class BigramSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

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

    public static class TopKMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, TopKHeap> localTop = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length != 3) {
                return;
            }
            String doc = parts[0];
            String bigram = parts[1];
            int count = Integer.parseInt(parts[2]);

            TopKHeap heap = localTop.get(doc);
            if (heap == null) {
                heap = new TopKHeap();
                localTop.put(doc, heap);
            }
            heap.add(bigram, count);
        }

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

    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outVal = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TopKHeap globalTop = new TopKHeap();

            for (Text val : values) {
                String s = val.toString();
                int sep = s.lastIndexOf('\t');
                String bigram = s.substring(0, sep);
                int count = Integer.parseInt(s.substring(sep + 1));
                globalTop.add(bigram, count);
            }

            for (BigramEntry entry : globalTop.getSortedDescending()) {
                outVal.set(entry.bigram + "\t" + entry.count);
                context.write(key, outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: Q3 <input path> <output path> [stopwords HDFS path]");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tempPath = new Path(args[1] + "_tmp_counts");
        String stopwordsPath = (args.length == 3) ? args[2] : "/user/root/stopwords/english";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(tempPath)) {
            fs.delete(tempPath, true);
        }

        Job job1 = Job.getInstance(conf, "Q3 Job 1 - Bigram Count");
        job1.setJarByClass(Q3.class);
        job1.addCacheFile(new URI(stopwordsPath + "#stopwords"));

        job1.setMapperClass(BigramMapper.class);
        job1.setCombinerClass(BigramSumReducer.class);
        job1.setReducerClass(BigramSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tempPath);

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Q3 Job 2 - Top-10 Bigrams per File");
        job2.setJarByClass(Q3.class);

        job2.setMapperClass(TopKMapper.class);
        job2.setReducerClass(TopKReducer.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, outputPath);

        boolean success = job2.waitForCompletion(true);

        fs.delete(tempPath, true);

        System.exit(success ? 0 : 1);
    }
}