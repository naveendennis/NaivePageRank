package org.dennis.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author Naveen Dennis Barnabas
 * @email nbarnaba@uncc.edu
 * @studentid 800950806
 **/
public class LinkGraph extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(LinkGraph.class);
    private static int numberOfPages ;
    private static Set<String> allPageIds = new TreeSet<>();
    private static double decayValue ;
    private static final String DELIMITER = "#%^^%#";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LinkGraph(), args);
        System.exit(res);
    }

    private static String getValueIn(String tag, String line){
        String openingTag = "<"+tag+">";
        String closingTag = "</"+tag+">";
        final Pattern title_pattern = Pattern.compile(openingTag+"(.+?)"+closingTag);
        final Matcher title_matcher = title_pattern.matcher(line);
        title_matcher.find();
        return title_matcher.group(1);
    }

    private static String putValueIn(String tag, String value){
        String openingTag = "<"+tag+">";
        String closingTag = "</"+tag+">";
        return openingTag+value.trim()+closingTag;
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " org.dennis.pagerank.LinkGraph ");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (args.length == 2){
            decayValue = 0.85;
        }else {
            decayValue = Double.parseDouble(args[2]);
        }
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            /**
             * name of the file from where the data is extracted. Determined using the path.
             */
            String wikiLine = lineText.toString();
            Pattern linkPat = Pattern .compile("\\[\\[.*?]\\]");
            Matcher m = linkPat.matcher(wikiLine); // extract outgoing links
            String pageName = getValueIn("title", wikiLine).trim();
            allPageIds.add(pageName);
            while(m.find()) { // loop on each outgoing link
                String url = m.group()
                        .replace("[[", "")
                        .replace("]]", ""); // drop the brackets and any nested ones if any
                if(url!=null && !url.isEmpty()) {
                    allPageIds.add(url.trim());
                    context.write(new Text(pageName), new Text(url.trim()));
                    LOG.info("Mapper => "+ pageName +": "+url );
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text pageId, Iterable<Text> outLink, Context context)
                throws IOException, InterruptedException {
            numberOfPages = allPageIds.size();
            List<String> outLinkIds = new ArrayList<>();
            int noOfOutLLinks;
            for(Text eachOutLink: outLink){
                if (eachOutLink !=null && !eachOutLink.toString().isEmpty()) {
                    LOG.info("Reducer => Outlink -> "+eachOutLink);
                    outLinkIds.add(eachOutLink.toString().trim());
                }
            }
            List<Double> initialRankVector = new ArrayList<>(
                    Collections.nCopies(numberOfPages, (1/(double)numberOfPages)));
            noOfOutLLinks = outLinkIds.size();
            StringBuffer outLinkFormatted = new StringBuffer();
            StringBuffer initialRankOutput = new StringBuffer();
            int index = 0;
            for (String eachPageId : allPageIds){
                double currentValue = (1/numberOfPages)*(1-decayValue);
                if(outLinkIds.contains(eachPageId)){
                    currentValue+= decayValue*(1/noOfOutLLinks);
                }
                initialRankOutput.append(initialRankVector.get(index).toString()+DELIMITER);
                outLinkFormatted.append(currentValue+DELIMITER);
                index++;
            }

//            LOG.info("Reducer => Stochastic Vector -> "+outLinkFormatted.toString());
//            LOG.info("Reducer => Initial Rank Vector -> "+initialRankOutput.toString());

            String result = putValueIn("outlinks", outLinkFormatted.toString());
            result += putValueIn("initialRankVector", initialRankOutput.toString());
            context.write(pageId, new Text(result));

        }
    }
}