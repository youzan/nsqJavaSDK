package com.youzan.util;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IOUtil {

    private static final Logger logger = LoggerFactory.getLogger(IOUtil.class);

    public static final String ASCII = "US-ASCII";
    public static final String UTF = "UTF-8";
    public static final String DEFAULT_CHARSET_NAME = UTF;

    static {
        // detection
        try {
            UTF.getBytes(UTF);
        } catch (UnsupportedEncodingException e) {
            logger.error("UTF-8 charset is not supported by your JVM?", e);
        }

        try {
            ASCII.getBytes(ASCII);
        } catch (UnsupportedEncodingException e) {
            logger.error("US-ASCII charset is not supported by your JVM?", e);
        }
    }

    public final static void closeQuietly(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            if (null != closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    logger.error("Exception:", e);
                }
            }
        }
    }

    public final static void closeQuietly(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (null != closeable) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    logger.error("Exception:", e);
                }
            }
        }
    }
}
