/**
 * 
 */
package com.youzan.nsq.client.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhaoxi (linzuxiong)
 * @email linzuxiong1988@gmail.com
 *
 */
public class Feature {
    private static final Logger logger = LoggerFactory.getLogger(Feature.class);

    private int max_rdy_count;
    private String version;
    private long max_msg_timeout;
    private long msg_timeout;
    private boolean tls_v1;
    private boolean deflate;
    private int deflate_level;
    private int max_deflate_level;
    private boolean snappy;
    private int sample_rate;
    private boolean auth_required;
    private int output_buffer_size;
    private int output_buffer_timeout;

}
