package com.youzan.nsq.client.entity;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * set desired tag for consumer would lile to receive, a valid tag is:
 * smaller than 100 bytes in length and it is th combination of alphabet[a-zA-Z] and number[0-9]
 */
public class DesiredTag {
    //tag limitation in length
    private static final int TAG_FILTER_LIMIT = 100;
    private static final Pattern VALID_TAG_REFEX = Pattern.compile("^[a-zA-Z0-9_-]+$");
    private static final Logger logger = LoggerFactory.getLogger(DesiredTag.class);

    //default tag value is empty, it receives any messages in topic/channel
    private String tag = "";

    /**
     *
     * @param tag string to initialize as tag filter
     * @throws IllegalArgumentException thrown when passin filter is not valid.
     */
    public DesiredTag(String tag) throws IllegalArgumentException {
        if (null != tag && tag.isEmpty())
            return;
        if(validateTag(tag)) {
            this.tag = tag;
        } else {
            throw new IllegalArgumentException("Desired tag: " + tag + " is invalid.");
        }
    }

    private boolean validateTag(String tagFilter) {
        if(tagFilter.length() > TAG_FILTER_LIMIT) {
            logger.error("Length of tag filter should not exceed " + TAG_FILTER_LIMIT + " bytes");
            return false;
        }
        return VALID_TAG_REFEX.matcher(tagFilter).find();
    }

    public boolean match(final DesiredTag tag) {
        if(StringUtils.isEmpty(tag.tag))
            return false;
        return this.tag.equals(tag.tag);
    }

    public String toString() {
        return this.tag;
    }

    /**
     * get tag name
     * @return tag name
     */
    public String getTagName() {
        return this.tag;
    }

    public ExtVer version() {
        return ExtVer.Ver0x2;
    }
}
