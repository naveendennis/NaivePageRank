package org.dennis.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static org.dennis.utils.Utils.*;

/**
 * @author Naveen Dennis Barnabas
 * @email nbarnaba@uncc.edu
 * @studentid 800950806
 **/
public class PageRank extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(PageRank.class);
    private static int numberOfIterations;
    private static double decayValue;
    private static java.util.Map<String, String> pageRankTable = new HashMap<>();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PageRank(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        int result = 1;
        String currentInputFile = args[0];
        Configuration currentConfig = getConf();
        for(int i = 0 ; i < 10 ; i++) {
            String outputFile = args[1]+"_"+i;
            deleteFile(currentConfig, new String[]{TEMP_PAGE_RANK, args[1]});

            /**
             * Calculate the value of the initial PR Vector (Extract from Document)
             */
            Job populatePageRankJob = Job.getInstance(currentConfig);
            populatePageRankJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(populatePageRankJob, currentInputFile);
            FileOutputFormat.setOutputPath(populatePageRankJob, new Path(TEMP_PAGE_RANK));
            if (args.length == 2) {
                numberOfIterations = 10;
            } else {
                numberOfIterations = Integer.parseInt(args[2]);
            }

            if (args.length == 3) {
                decayValue = 0.85;
            } else {
                decayValue = Double.parseDouble(args[3]);
            }
            populatePageRankJob.setMapperClass(PRInitializeMap.class);
            populatePageRankJob.setReducerClass(PRInitializeReduce.class);
            populatePageRankJob.setMapOutputKeyClass(Text.class);
            populatePageRankJob.setMapOutputValueClass(Text.class);
            populatePageRankJob.setOutputKeyClass(Text.class);
            populatePageRankJob.setOutputValueClass(Text.class);

            result &= populatePageRankJob.waitForCompletion(true) ? 0 : 1;
            /**
             *  Using the map as lookup calculate the page rank
             */
            deleteFile(currentConfig, TEMP_PAGE_RANK);
            Job computePRJob = Job.getInstance(currentConfig);
            computePRJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(computePRJob, currentInputFile);
            FileOutputFormat.setOutputPath(computePRJob, new Path(TEMP_PAGE_RANK+"_"+i));
            computePRJob.setMapperClass(PRComputeMap.class);
            computePRJob.setReducerClass(PRComputeReduce.class);
            computePRJob.setMapOutputKeyClass(Text.class);
            computePRJob.setMapOutputValueClass(DoubleWritable.class);
            computePRJob.setOutputKeyClass(Text.class);
            computePRJob.setOutputValueClass(DoubleWritable.class);

            result &= computePRJob.waitForCompletion(true) ? 0 : 1;

            /**
             * Update the page rank
             */
            Job prUpdateJob = Job.getInstance(currentConfig);
            prUpdateJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(prUpdateJob, currentInputFile);
            FileOutputFormat.setOutputPath(prUpdateJob, new Path(outputFile));
            prUpdateJob.setMapperClass(PRUpdateMap.class);
            prUpdateJob.setReducerClass(PRUpdateReduce.class);
            prUpdateJob.setMapOutputKeyClass(Text.class);
            prUpdateJob.setMapOutputValueClass(Text.class);
            prUpdateJob.setOutputKeyClass(Text.class);
            prUpdateJob.setOutputValueClass(Text.class);

            result &= prUpdateJob.waitForCompletion(true) ? 0 : 1;

            currentInputFile = outputFile;
            pageRankTable.clear();

        }

        return result;
    }

    public static class PRInitializeMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String[] lineContents = lineText.toString().split(START_DELIMITER);
            String key = lineContents[0].trim();
            String content = lineContents[1].trim();
            String pageRankValue = getValueIn(PAGE_RANK_TAG, content.toString());
            pageRankTable.put(key, pageRankValue);
//            context.write(new Text(key), new Text(pageRankValue));
        }

    }

    public static class PRInitializeReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text pageId, Iterable<Text> outLink, Context context)
                throws IOException, InterruptedException {
//            for (Text each : outLink) {
//                context.write(pageId, each);
//            }
        }
    }

    public static class PRComputeMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String[] lineContents = lineText.toString().split(START_DELIMITER);
            Text key = new Text(lineContents[0].trim());
            String content = lineContents[1].trim();
            String[] outLinksList = getValueIn(OUTLINKS_TAG, content.toString()).split(DELIMITER);
            int outLinksLength = outLinksList.length;
            for (String eachOutlink : outLinksList) {
                context.write(key, new DoubleWritable(
                        Double.parseDouble(
                                pageRankTable.get(eachOutlink)) / outLinksLength));
            }

        }
    }

    public static class PRComputeReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text pageId, Iterable<DoubleWritable> outLink, Context context)
                throws IOException, InterruptedException {
            double pageRank = 0d;
            for (DoubleWritable value : outLink) {
                pageRank += value.get();
            }
            pageRank = pageRank * decayValue + (1 - decayValue);
            pageRankTable.put(pageId.toString(), String.valueOf(pageRank));
            context.write(pageId, new DoubleWritable(pageRank));
        }
    }

    public static class PRUpdateMap extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String[] lineContents = lineText.toString().split(START_DELIMITER);
            String key = lineContents[0].trim();
            String content = lineContents[1].trim();
            content = updateValueIn(PAGE_RANK_TAG, content, pageRankTable.get(key));
            context.write(new Text(key), new Text(content));

        }
    }

    public static class PRUpdateReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text pageId, Iterable<Text> content, Context context)
                throws IOException, InterruptedException {
            for (Text eachContent: content){
                context.write(pageId, eachContent);
            }
        }
    }
}