package org.dennis.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dennis on 10/29/17.
 */
public class Utils {
    private static final Logger LOG = Logger.getLogger(Utils.class);
    public static final String DELIMITER = "####";
    public static final String START_DELIMITER = "#!#!#";
    public static final String OUTLINK_SIZE_TAG = "outlinkSize";
    public static final String PAGE_RANK_TAG = "pageRank";
    public static final String OUTLINKS_TAG = "outlinks";
    public static final String TITLE = "title";
    public static final String TEMP_PAGE_RANK = "output/temp_pr";
    public static final String LINK_GRAPH = "output/pagerank/link_graph";

    private static String getStartTag(String tagName){
        return "<"+tagName+">";
    }

    private static String getEndTag(String tagName){
        return "</"+tagName+">";
    }
    
    public static String getValueIn(String tag, String line){
        String openingTag = getStartTag(tag);
        String closingTag = getEndTag(tag);
        final Pattern title_pattern = Pattern.compile(openingTag+"(.+?)"+closingTag);
        final Matcher title_matcher = title_pattern.matcher(line);
        title_matcher.find();
        return title_matcher.group(1);
    }

    public static String putValueIn(String tag, String value){
        String openingTag = getStartTag(tag);
        String closingTag = getEndTag(tag);
        return openingTag+value.trim()+closingTag;
    }

    public static String updateValueIn(String tag, String line, String newValue){
        String newTagValue = putValueIn(tag, newValue);
        int firstIndex = line.indexOf(getStartTag(tag));
        int lastIndex = line.indexOf(getEndTag(tag));
        return line.substring(firstIndex)+newTagValue+line.substring(lastIndex+1, line.length());
    }
    
    public static int getCount(String value, String delimiter){
        return value.split(delimiter).length;
    }

    public static void deleteFile(Configuration config, String fileName){
        try {
            FileSystem fs = FileSystem.get(config);
            if (fs.exists(new Path(fileName))) {
            /*If exist delete the output path*/
                fs.delete(new Path(fileName), true);
            }
        }catch (IOException e){
            LOG.error("Error reading fileSystem during cleanUp");
        }
    }

    public static void deleteFile(Configuration config, String[] fileNames) {
        for (String eachFileName : fileNames) {
            deleteFile(config, eachFileName);
        }
    }

    public static void renameFile(Configuration config, String fileName, String newFileName){
        try {
            FileSystem fs = FileSystem.get(config);
            if (fs.exists(new Path(fileName))) {
            /*If exist delete the output path*/
                fs.rename(new Path(fileName), new Path(newFileName));
            }
        }catch (IOException e){
            LOG.error("Error reading fileSystem during cleanUp");
        }
    }

}
