package com.youzan.nsq.client.entity;

import com.youzan.nsq.client.MessageMetadata;
import com.youzan.nsq.client.Producer;
import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.core.Client;
import com.youzan.nsq.client.core.NSQConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lin on 16/9/8.
 */
public class TraceLogger {
   private static final Logger logger = LoggerFactory.getLogger(TraceLogger.class);

   public static final String TRAE_LOGGER_NAME = ConfigAccessAgent.getProperty("nsq.sdk.message.trace.log");
   private static final Logger trace = LoggerFactory.getLogger(TRAE_LOGGER_NAME);

   private static final String DEFAULT_MSG_REV_TRACE_FORMAT = "Client: %s <= NSQd: %s\n\tMessage meta-data: %s";
   private static final String DEFAULT_MSG_SEN_TRACE_FORMAT = "Client: %s => NSQd: %s\n\tMessage meta-data: %s";

   public static boolean isTraceLoggerEnabled(){
      return trace.isDebugEnabled();
   }
   /**
    * static function to record trace of pass in {@link Message} message in pass in client
    * @param client
    * @param msg
    */
   public static void trace(final Client client, final NSQConnection nsqd, final MessageMetadata msg){
      String traceMsg;
      if(client instanceof Producer) {
         traceMsg = String.format(DEFAULT_MSG_SEN_TRACE_FORMAT, client.toString(), nsqd.getAddress().toString(), msg.toMetadataStr());
      }else {
         traceMsg = String.format(DEFAULT_MSG_REV_TRACE_FORMAT, client.toString(), nsqd.getAddress().toString(), msg.toMetadataStr());
      }
      trace.debug(traceMsg);
   }
}
