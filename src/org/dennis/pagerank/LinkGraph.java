package org.dennis.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import static org.dennis.utils.Utils.*;

/**
 * @author Naveen Dennis Barnabas
 * @email nbarnaba@uncc.edu
 * @studentid 800950806
 **/
public class LinkGraph extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(LinkGraph.class);
    private static int numberOfPages ;
    /**
     * used for calculating the page ids
     */
    private static Set<String> allPageIds = new TreeSet<>();

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " org.dennis.pagerank.LinkGraph ");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, getFilePath(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Extracts the url outlinks and places each outlink in a key value pair <pageid, outlink>
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            /**
             * name of the file from where the data is extracted. Determined using the path.
             */
            String wikiLine = lineText.toString();
            Pattern linkPat = Pattern .compile("\\[\\[.*?]\\]");
            Matcher m = linkPat.matcher(wikiLine); // extract outgoing links
            String pageName = getValueIn(TITLE, wikiLine).trim();
            allPageIds.add(pageName);
            while(m.find()) { // loop on each outgoing link
                String url = m.group()
                        .replace("[[", "")
                        .replace("]]", ""); // drop the brackets and any nested ones if any
                if(url!=null && !url.isEmpty()) {
                    allPageIds.add(url.trim());
                    context.write(getText(pageName), getText(url.trim()));
                    LOG.info("Mapper => "+ pageName +": "+url );
                }
            }
        }
    }

    /**
     * <outlinks></outlinks><pagerank></pagerank><numberofoutlinks></numberofoutlinks>
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text pageId, Iterable<Text> outLink, Context context)
                throws IOException, InterruptedException {
            numberOfPages = allPageIds.size();
            LOG.info("Number of Pages => "+numberOfPages);
            List<String> outLinkIds = new ArrayList<>();
            StringBuffer outlinkStrBuff = new StringBuffer();
            for(Text eachOutLink: outLink){
                if (eachOutLink !=null && !eachOutLink.toString().isEmpty()) {
                    outLinkIds.add(eachOutLink.toString().trim());
                    outlinkStrBuff.append(eachOutLink+DELIMITER);
                }
            }


            String result = RECORD_DELIMITER+putValueIn(PAGE_RANK_TAG, String.valueOf(1/(double)numberOfPages));
            result += putValueIn(OUTLINKS_TAG, outlinkStrBuff.toString());
            context.write(pageId, getText(result));

        }
    }
}