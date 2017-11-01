package org.dennis.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

import static org.dennis.utils.Utils.*;

/**
 * @author Naveen Dennis Barnabas
 * @email nbarnaba@uncc.edu
 * @studentid 800950806
 **/
public class PageRank{
    /**
     * LG_RECORD_SEPERATOR - pattern used to denote the seperation between the <key, value>
     *
     * OUTLINK_SEPERATOR - pattern used to denote the separation between the outlinks within the outlinks tag
     *
     * TEMP_PAGE_RANK_PATH, LINK_GRAPH_PATH, RECONTRUCT_GRAPH_PATH are the three paths where files will be stored for
     * every iteration.
     *
     * TEMP_PAGE_RANK_PATH - Used to store the page ranks at each iteration
     *
     * LINK_GRAPH_PATH - Used to store the link graph with the updated page ranks in the end of every iteration
     *
     * RECONTRUCT_GRAPH_PATH - Used to store the updated link graph temporarily.
     */
    private static final Pattern LG_RECORD_SEPERATOR = Pattern.compile(RECORD_DELIMITER);
    private static final Pattern OUTLINK_SEPARATOR = Pattern.compile(DELIMITER);
    private static final Logger LOG = Logger.getLogger(PageRank.class);
    private static final double DECAY = 0.85d;
    public static final Path TEMP_PAGE_RANK_PATH = new Path(TEMP_PAGE_RANK_LOC);
    public static final Path LINK_GRAPH_PATH = new Path(LINK_GRAPH_LOC);
    public static final Path RECONTRUCT_GRAPH_PATH = new Path(RECONTRUCT_GRAPH_LOC);


    /**
     * Record Parser makes it easy to extract all the details about the graph from the LinkGraph file. Extensively used
     * in extraction just before sorting.
     */
    public static class RecordParser{
        String pageId;
        String[] outLinks;
        Double pageRank;

        public RecordParser(String line){
            String [] attr = LG_RECORD_SEPERATOR.split(line);
            this.pageId = attr[0].trim();
            this.outLinks = OUTLINK_SEPARATOR.split(getValueIn(OUTLINKS_TAG, attr[1].trim()));
            this.pageRank = Double.valueOf(getValueIn(PAGE_RANK_TAG, attr[1].trim()));
        }

    }

    /**
     * The entire page rank computation is triggered from there using three Driver classes. They are LinkGraph,
     * PageRanksDriver, ReconstructionFileDriver and SorterDriver.
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        int statusCode;
        statusCode = ToolRunner.run(new LinkGraph(), new String[]{args[0], LINK_GRAPH_LOC});
        for(int iterator = 0 ; iterator < 10; iterator++) {
            if (statusCode == 0) {
                statusCode = ToolRunner.run(new PageRanksDriver(), new String[]{LINK_GRAPH_LOC, TEMP_PAGE_RANK_LOC});
            }
            if (statusCode == 0) {
                statusCode = ToolRunner.run(new ReconstructFileDriver(),
                        new String[]{LINK_GRAPH_LOC, TEMP_PAGE_RANK_LOC, RECONTRUCT_GRAPH_LOC});
            }
        }
        if (statusCode == 0){
            statusCode = ToolRunner.run(new SorterDriver(), new String[]{LINK_GRAPH_LOC, args[1]});
        }
        System.exit(statusCode);
    }

    /**
     * This driver triggers the operation to construct a pagerank vector for each iteration from LINK_GRAPH_PATH.
     * The output is stored in TEMP_PAGE_RANK_PATH.
     */
    static class PageRanksDriver extends Configured implements Tool{

        @Override
        public int run(String[] args) throws Exception {
            Job job = Job.getInstance(getConf(), " org.dennis.pagerank.PageRanksDriver ");
            job.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(job, LINK_GRAPH_LOC);
            FileOutputFormat.setOutputPath(job, TEMP_PAGE_RANK_PATH);
            job.setMapperClass(PRRemapper.class);
            job.setReducerClass(PRCalculator.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            return job.waitForCompletion(true) ? 0 : 1;
        }

        /**
         * Emits the values by <outlink-page, page-rank(current_page)/len(outlinks)>
         */
        public static class PRRemapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

            public void map(LongWritable offset, Text lineText, Context context)
                    throws IOException, InterruptedException {
                LOG.info("PRREMAPPER => "+lineText.toString());
                RecordParser record = new RecordParser(lineText.toString());
                for (String eachOutLink: record.outLinks){
                    context.write(getText(eachOutLink), new DoubleWritable(record.pageRank/record.outLinks.length));
                }
            }
        }

        /**
         * This calculates the PAGE_RANK_LOC in each iteration
         */
        public static class PRCalculator extends Reducer<Text, DoubleWritable, Text, Text> {

            @Override
            public void reduce(Text pageId, Iterable<DoubleWritable> pageRanks, Context context)
                    throws IOException, InterruptedException {
                double result = 0d;
                // sum up all the pageranks
                for(DoubleWritable eachPageRank: pageRanks){
                    result += eachPageRank.get();
                }
                // compute with decay
                result = (1-DECAY) + DECAY * result;
                context.write(pageId, getText(RECORD_DELIMITER+putValueIn(NEW_PAGE_RANK_TAG, String.valueOf(result))));
            }
        }

    }

    /**
     * This driver takes multiple inputs from two locations : LINK_GRAPH_PATH and TEMP_PAGE_RANK_PATH.
     *
     * The output is stored in RECONTRUCT_GRAPH_PATH. Since this is iterated.
     * RECONTRUCT_GRAPH_PATH is renamed to LINK_GRAPH_PATH after each iteration and TEMP_PAGE_RANK_PATH is deleted to
     * avoid clashes in naming.
     */
    static class ReconstructFileDriver extends Configured implements Tool{

        @Override
        public int run(String[] args) throws Exception {
            Configuration config = getConf();
            Job job = Job.getInstance(config, " org.dennis.pagerank.ReconstructFile ");
            job.setJarByClass(this.getClass());

            MultipleInputs.addInputPath(job, LINK_GRAPH_PATH, TextInputFormat.class, LinkGraphParseMapper.class);
            MultipleInputs.addInputPath(job, TEMP_PAGE_RANK_PATH, TextInputFormat.class, LinkGraphParseMapper.class);
            FileOutputFormat.setOutputPath(job, RECONTRUCT_GRAPH_PATH);
            job.setReducerClass(ReconstructGraph.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            int result= job.waitForCompletion(true) ? 0 : 1;
            /*
            After each iteration the RECONTRUCT_GRAPH_PATH obtained from this step is renamed as LINK_GRAPH_PATH.

             */
            FileSystem hdfs = FileSystem.get(config);
            // delete existing directory
            if (hdfs.exists(LINK_GRAPH_PATH)) {
                hdfs.delete(LINK_GRAPH_PATH, true);
                hdfs.delete(TEMP_PAGE_RANK_PATH, true);
                hdfs.rename(RECONTRUCT_GRAPH_PATH, LINK_GRAPH_PATH);
            }
            return result;
        }

        /**
         * READ the file and splits them into key value pairs using LG_RECORD_SEPARATOR.
         * Used for reading LINK_GRAPH_LOC and PAGE_RANK_LOC.
         */
        public static class LinkGraphParseMapper extends Mapper<LongWritable, Text, Text, Text> {

            public void map(LongWritable offset, Text lineText, Context context)
                    throws IOException, InterruptedException {
                String[] attr = LG_RECORD_SEPERATOR.split(lineText.toString());
                context.write(getText(attr[0].trim()), getText(attr[1].trim()));
            }
        }

        /**
         * This class is used to merge files from two different locations and update the pagerank. So, when the
         * iteration count / number of values per key is 2 then the new pagerank is updated. If the originalLine
         * contains PAGE_RANK_TAG then it is a graph with no in-links and so the page-rank is set as (1-d). If
         * originalLine is null, then the page is a sink which is not considered for page-rank calculation in this
         * assignment.
         */
        public static class ReconstructGraph extends Reducer<Text, Text, Text, Text> {

            @Override
            public void reduce(Text pageId, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                String originalLine = null;
                String newPageRank = null;
                long noOfValues = 0;
                for(Text eachValue: values){
                    noOfValues++;
                    String currentValue = eachValue.toString();
                    if (!getValueIn(NEW_PAGE_RANK_TAG, currentValue).isEmpty()){
                        // value from PAGE_RANK_LOC
                        newPageRank = getValueIn(NEW_PAGE_RANK_TAG, currentValue);
                    }else{
                        // value from LINK_GRAPH_LOC
                        originalLine = currentValue;
                    }
                }
            /*
             * To make sure that only outlink entries which have intial pagerank values are emitted
             */
                if (noOfValues == 2) {
                    // These are normal entries which are found in the LINK_GRAPH with both incoming and outgoing edges
                    context.write(pageId, getText(RECORD_DELIMITER +
                            updateValueIn(PAGE_RANK_TAG, originalLine, newPageRank)));
                }else if(originalLine!=null && !getValueIn(PAGE_RANK_TAG, originalLine).isEmpty()){
                    // These are entries which have no incoming edges so that page_rank is set as (1-d) since no inlinks are
                    // available for them.
                    LOG.info("ORIGINAL LINE => "+originalLine);
                    context.write(pageId, getText(RECORD_DELIMITER +
                            updateValueIn(PAGE_RANK_TAG, originalLine, String.valueOf((1-DECAY)))));
                }
            /*
            Pages that do not abide by these are sinks with no outgoing edges and they are not considered for this
             problem
             */
            }
        }

    }

    /**
     * This driver sorts the values
     */
    static class SorterDriver extends Configured implements Tool{

        @Override
        public int run(String[] args) throws Exception {
            Configuration currentConfiguration = getConf();
            Job job = Job.getInstance(currentConfiguration, " org.dennis.pagerank.SorterDriver ");
            job.setJarByClass(this.getClass());

            FileInputFormat.addInputPaths(job, LINK_GRAPH_LOC);
            FileOutputFormat.setOutputPath(job, getFilePath(args[1]));
            job.setMapperClass(ExtractParser.class);
            job.setReducerClass(Identity.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            job.setNumReduceTasks(1);
            int result = job.waitForCompletion(true) ? 0 : 1;
            /*
             * Cleans up the link graph before execution terminates
             */
            cleanUp(FileSystem.get(currentConfiguration), LINK_GRAPH_PATH);
            return result;
        }

        /**
         * Used as part of the sorter to extract values by <pageId, pageRank>
         */
        public static class ExtractParser extends Mapper<LongWritable, Text, DoubleWritable, Text> {

            public void map(LongWritable offset, Text lineText, Context context)
                    throws IOException, InterruptedException {
                RecordParser recordParser = new RecordParser(lineText.toString());
                context.write(new DoubleWritable(recordParser.pageRank), getText(recordParser.pageId) );
            }
        }

        /**
         * When sorting the values are just passed as Identity
         */
        public static class Identity extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
            @Override
            public void reduce(DoubleWritable pageRank, Iterable<Text> pageId, Context context)
                    throws IOException, InterruptedException {
                for (Text eachPageId: pageId){
                    context.write(eachPageId, pageRank);
                }
            }
        }
    }


}