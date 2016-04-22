package com.youzan.util;

import java.io.Closeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IOUtil {

    private static final Logger logger = LoggerFactory.getLogger(IOUtil.class);

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
