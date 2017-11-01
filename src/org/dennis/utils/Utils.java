package org.dennis.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility Functions for PageRank and LinkGraph
 * Created by dennis on 10/29/17.
 */
public class Utils {
    private static final Logger LOG = Logger.getLogger(Utils.class);
    public static final String DELIMITER = "####";
    public static final String RECORD_DELIMITER = "#!#!#";
    public static final String PAGE_RANK_TAG = "pageRank";
    public static final String NEW_PAGE_RANK_TAG = "new_pageRank";
    public static final String OUTLINKS_TAG = "outlinks";
    public static final String TITLE = "title";
    public static final String TEMP_LOC = "output/";
    public static final String TEMP_PAGE_RANK_LOC = TEMP_LOC+"temp_page_rank";
    public static final String LINK_GRAPH_LOC = TEMP_LOC+"link_graph";
    public static final String RECONTRUCT_GRAPH_LOC = TEMP_LOC+"recontruct_graph";

    /**
     * Encloses the provided tagName within <>
     * @param tagName
     * @return <tagName>
     */
    private static String getStartTag(String tagName){
        return "<"+tagName+">";
    }

    /**
     * Encloses provided tagName within </>
     * @param tagName
     * @return </tagName>
     */
    private static String getEndTag(String tagName){
        return "</"+tagName+">";
    }

    /**
     * Used to extract value within a tag
     * @param tag name of the tag
     * @param line The line containing the tag
     * @return empty string if not found otherwise returns the text within tag
     */
    public static String getValueIn(String tag, String line){
        String openingTag = getStartTag(tag);
        String closingTag = getEndTag(tag);
        final Pattern title_pattern = Pattern.compile(openingTag+"(.+?)"+closingTag);
        final Matcher title_matcher = title_pattern.matcher(line);
        title_matcher.find();
        try {
            return title_matcher.group(1);
        }catch (IllegalStateException e){
            LOG.info("Skipping page - outlink page which has no pagerank is possibly detected...");
            return "";
        }
    }

    /**
     * Used to insert value within a specified tag
     * @param tag TagName
     * @param value value to be enclosed by tag
     * @return a string - <tag>value</tag>
     */
    public static String putValueIn(String tag, String value){
        String openingTag = getStartTag(tag);
        String closingTag = getEndTag(tag);
        return openingTag+value.trim()+closingTag;
    }

    /**
     * Used to update the value within a tag and return the entire updated document
     * @param tag tagName
     * @param line Entire Line containing the tag
     * @param newValue new value that the old value is to be replaced with
     * @return updated document
     */
    public static String updateValueIn(String tag, String line, String newValue){
        int startIndex = line.lastIndexOf(getStartTag(tag))+tag.length()+2; // Start Index of old value
        int endIndex = line.indexOf(getEndTag(tag)); // end index of old value
        StringBuilder result = new StringBuilder(line);
        result.replace(startIndex, endIndex, newValue); // replace old value with new value
        return result.toString();
    }

    /**
     * Returns a new Path Object
     * @param fileName
     * @return pathObject
     */
    public static Path getFilePath(String fileName){
        return new Path(fileName);
    }


    /**
     * Returns a new Text Object
     * @param value
     * @return Text object
     */
    public static Text getText(String value){
        return new Text(value);
    }


}
