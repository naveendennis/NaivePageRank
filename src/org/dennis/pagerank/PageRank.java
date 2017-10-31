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
import java.util.regex.Matcher;
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
    private static final Pattern RECORD_SEPERATOR = Pattern.compile("(.+)"+START_DELIMITER+"(.+)");

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new PageRank(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        int res = ToolRunner.run(new LinkGraph(), new String[]{args[0], LINK_GRAPH});
        if (res == 0 ){
            res = ToolRunner.run(new PRInitialize(), new String[]{LINK_GRAPH, TEMP_PAGE_RANK});
        }
        String initialLocation = LINK_GRAPH;
        for (int i = 0 ; i <= 10 ; i++) {

            if (res == 0) {
                res = ToolRunner.run(new PRCompute(),
                        new String[]{initialLocation, TEMP_PAGE_RANK+"_d_"+i});
            }
            if (res == 0) {
                res = ToolRunner.run(new PRUpdate(),
                        new String[]{initialLocation, TEMP_PAGE_RANK + "_" + i});
            }
            initialLocation = TEMP_PAGE_RANK+"_"+i;
        }
        renameFile(getConf(), initialLocation, args[1]);
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
            FileOutputFormat.setOutputPath(populatePageRankJob, new Path(args[1]));

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
                    Matcher m1 = RECORD_SEPERATOR.matcher(lineText.toString());
                    String key = null;
                    String content = null;
                    while(m1.find()){
                        key = m1.group(1).trim();
                        content = m1.group(2);
                    }
                    LOG.info("INitmapper => key: "+lineText.toString());
                    String pageRankValue = getValueIn(PAGE_RANK_TAG, content.toString());
                    pageRankTable.put(key, pageRankValue);
                    context.write(new Text(key), new Text(pageRankValue));
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
            FileOutputFormat.setOutputPath(computePRJob, new Path(args[1]));
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
                Matcher m1 = RECORD_SEPERATOR.matcher(lineText.toString());
                String key = null;
                String content = null;
                while(m1.find()){
                    key = m1.group(1).trim();
                    content = m1.group(2);
                }
                String[] outLinksList = getValueIn(OUTLINKS_TAG, content.toString()).split(DELIMITER);
                int outLinksLength = outLinksList.length;
                for (String eachOutlink : outLinksList) {
                    context.write(new Text(key), new DoubleWritable(
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
            FileOutputFormat.setOutputPath(prUpdateJob, new Path(args[1]));
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
                Matcher m1 = RECORD_SEPERATOR.matcher(lineText.toString());
                String key = null;
                String content = null;
                while(m1.find()){
                    key = m1.group(1).trim();
                    content = m1.group(2);
                }
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


}