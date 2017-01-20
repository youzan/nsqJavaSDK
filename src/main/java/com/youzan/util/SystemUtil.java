/**
 * 
 */
package com.youzan.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public final class SystemUtil {
    private static final Logger logger = LoggerFactory.getLogger(SystemUtil.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(processName.split("@")[0]);
    }
    public static ObjectMapper getObjectMapper(){
        return mapper;
    }
}
