package com.youzan.util;

import org.slf4j.Logger;

/**
 * Created by lin on 16/9/27.
 */
public class LogUtil {
    /**
     * debug msg into pass in logger, without log exception
     * @param logger
     * @param msg
     * @param objects
     */
    public static void warn(final Logger logger, final String msg, Object... objects){
        logger.debug(msg, objects);
    }
}
