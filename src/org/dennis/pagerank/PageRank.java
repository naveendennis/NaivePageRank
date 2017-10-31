package org.dennis.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
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
import java.util.regex.Pattern;

import static org.dennis.utils.Utils.*;

/**
 * @author Naveen Dennis Barnabas
 * @email nbarnaba@uncc.edu
 * @studentid 800950806
 **/
public class PageRank extends Configured implements Tool{

    private static final Logger LOG = Logger.getLogger(PageRank.class);
    private static final double decayValue = 0.85;
    private static java.util.Map<String, String> pageRankTable = new HashMap<>();
    private static final Pattern RECORD_SEPERATOR = Pattern.compile(START_DELIMITER);
    private static final Pattern OUTLINK_SEPERATOR = Pattern.compile(DELIMITER);

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new PageRank(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        int res = ToolRunner.run(new LinkGraph(), new String[]{args[0], LINK_GRAPH});

        String initialLocation = LINK_GRAPH;
        for (int i = 0 ; i < 10 ; i++) {
            if (res == 0 ){
                res = ToolRunner.run(new PRInitialize(), new String[]{initialLocation, TEMP_PAGE_RANK+"_i_"+i});
            }
            if (res == 0) {
                res = ToolRunner.run(new PRCompute(),
                        new String[]{initialLocation, TEMP_PAGE_RANK+"_d_"+i});
            }
            if (res == 0) {
                res = ToolRunner.run(new PRUpdate(),
                        new String[]{initialLocation, TEMP_PAGE_RANK + "_" + i});
            }
            initialLocation = TEMP_PAGE_RANK+"_"+i;
            pageRankTable.clear();
        }
        pageRankTable = null;

        if (res == 0) {
            res = ToolRunner.run(new Sorter(),
                    new String[]{initialLocation, args[1]});
        }
        return res;
    }

    public static class PRInitialize extends Configured implements Tool{
        @Override
        public int run(String[] args) throws Exception {
            /**
             * Calculate the value of the initial PR Vector (Extract from Document)
             */
            Configuration currentConfig = getConf();

            Job populatePageRankJob = Job.getInstance(currentConfig);
            populatePageRankJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(populatePageRankJob, args[0]);
            FileOutputFormat.setOutputPath(populatePageRankJob, getFilePath(args[1]));

            populatePageRankJob.setMapperClass(PRInitializeMap.class);
            populatePageRankJob.setReducerClass(PRInitializeReduce.class);
            populatePageRankJob.setMapOutputKeyClass(Text.class);
            populatePageRankJob.setMapOutputValueClass(Text.class);
            populatePageRankJob.setOutputKeyClass(Text.class);
            populatePageRankJob.setOutputValueClass(Text.class);

            return populatePageRankJob.waitForCompletion(true) ? 0 : 1;
        }

        public static class PRInitializeMap extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            public void map(LongWritable offset, Text lineText, Context context)
                    throws IOException, InterruptedException {
                    String [] values = RECORD_SEPERATOR.split(lineText.toString());
                    LOG.info("INitmapper => key: "+lineText.toString());
                    String key = values[0].trim();
                    String content = values[1];

                    String pageRankValue = getValueIn(PAGE_RANK_TAG, content.toString());
                    pageRankTable.put(key, pageRankValue);
                    context.write(getText(key), getText(pageRankValue));
            }

        }

        public static class PRInitializeReduce extends Reducer<Text, Text, Text, Text> {

            @Override
            public void reduce(Text pageId, Iterable<Text> outLink, Context context)
                    throws IOException, InterruptedException {
                for (Text each : outLink) {
                    context.write(pageId, each);
                }
            }
        }
    }

    public static class PRCompute extends Configured implements Tool{
        @Override
        public int run(String[] args) throws Exception {
            /**
             *  Using the map as lookup calculate the page rank
             */
            Configuration currentConfig = getConf();

            Job computePRJob = Job.getInstance(currentConfig);
            computePRJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(computePRJob, args[0]);
            FileOutputFormat.setOutputPath(computePRJob, getFilePath(args[1]));
            computePRJob.setMapperClass(PRComputeMap.class);
            computePRJob.setReducerClass(PRComputeReduce.class);
            computePRJob.setMapOutputKeyClass(Text.class);
            computePRJob.setMapOutputValueClass(DoubleWritable.class);
            computePRJob.setOutputKeyClass(Text.class);
            computePRJob.setOutputValueClass(DoubleWritable.class);

            return computePRJob.waitForCompletion(true) ? 0 : 1;
        }

        public static class PRComputeMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
            @Override
            public void map(LongWritable offset, Text lineText, Context context)
                    throws IOException, InterruptedException {
                String [] values = RECORD_SEPERATOR.split(lineText.toString());
                String key = values[0].trim();
                String content = values[1];
                String[] outLinksList = OUTLINK_SEPERATOR.split(getValueIn(OUTLINKS_TAG, content.toString()));
                int outLinksLength = outLinksList.length;
                for (String eachOutlink : outLinksList) {
                    LOG.info("PRComputeMap: "+eachOutlink.trim()+" => "+pageRankTable.get(eachOutlink.trim()));
                    if (pageRankTable.containsKey(eachOutlink.trim())) {
                        context.write(getText(key), new DoubleWritable(
                                Double.parseDouble(
                                        pageRankTable.get(eachOutlink.trim())) / outLinksLength));
                    }
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
    }

    public static class PRUpdate extends Configured implements Tool {
        @Override
        public int run(String[] args) throws Exception {
            /**
             * Update the page rank
             */
            Job prUpdateJob = Job.getInstance(getConf());
            prUpdateJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(prUpdateJob, args[0]);
            FileOutputFormat.setOutputPath(prUpdateJob, getFilePath(args[1]));
            prUpdateJob.setMapperClass(PRUpdateMap.class);
            prUpdateJob.setReducerClass(PRUpdateReduce.class);
            prUpdateJob.setMapOutputKeyClass(Text.class);
            prUpdateJob.setMapOutputValueClass(Text.class);
            prUpdateJob.setOutputKeyClass(Text.class);
            prUpdateJob.setOutputValueClass(Text.class);

            return prUpdateJob.waitForCompletion(true) ? 0 : 1;

        }
        public static class PRUpdateMap extends Mapper<LongWritable, Text, Text, Text> {
            public void map(LongWritable offset, Text lineText, Context context)
                    throws IOException, InterruptedException {
                String [] values = RECORD_SEPERATOR.split(lineText.toString());
                String key = values[0].trim();
                String content = values[1];
                content = updateValueIn(PAGE_RANK_TAG, content, pageRankTable.get(key));
                context.write(getText(key), getText(START_DELIMITER+content));

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

    public static class Sorter extends Configured implements Tool{
        @Override
        public int run(String[] args) throws Exception {
            /**
             *  Using the map as lookup calculate the page rank
             */
            Configuration currentConfig = getConf();

            Job sorterJob = Job.getInstance(currentConfig);
            sorterJob.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(sorterJob, args[0]);
            FileOutputFormat.setOutputPath(sorterJob, getFilePath(args[1]));
            sorterJob.setMapperClass(SorterMap.class);
            sorterJob.setReducerClass(SorterReduce.class);
            sorterJob.setMapOutputKeyClass(DoubleWritable.class);
            sorterJob.setMapOutputValueClass(Text.class);
            sorterJob.setOutputKeyClass(DoubleWritable.class);
            sorterJob.setOutputValueClass(Text.class);
            //set the comparator for sorting in decreasing order
            sorterJob.setSortComparatorClass(CustomDecreasingComparator.class);
            //important to set this since we need to perform one sort on the entire collection
            sorterJob.setNumReduceTasks(1);
            int result = sorterJob.waitForCompletion(true) ? 0 : 1;
            cleanUp(currentConfig, new String[] {TEMP_LOC});
            return result;
        }

        /**
         * Comparator is used to sort the output values in the decreasing order to show the highest ranked files first
         */

        static class CustomDecreasingComparator extends WritableComparator {

            public CustomDecreasingComparator() {
            }

            @SuppressWarnings("rawtypes")
            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                DoubleWritable a1 = (DoubleWritable)a;
                DoubleWritable b1 = (DoubleWritable)b;
                return -1 * a1.compareTo(b1);
            }
        }

        public static class SorterMap extends Mapper<LongWritable, Text, DoubleWritable, Text>{
            @Override
            public void map(LongWritable pageId, Text lineText, Context context)
                    throws IOException, InterruptedException {
                String [] values = RECORD_SEPERATOR.split(lineText.toString());
                String key = values[0].trim();
                String content = values[1];
                context.write(new DoubleWritable(Double.valueOf(getValueIn(PAGE_RANK_TAG, content).trim())),
                        getText(key));
            }
        }

        public static class SorterReduce extends Reducer<Text, DoubleWritable, DoubleWritable, Text>{

            @Override
            public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                    throws IOException, InterruptedException {
                for (DoubleWritable eachValue : values){
                    context.write(eachValue, key);
                }
            }
        }
    }

}