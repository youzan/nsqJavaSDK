package com.youzan.nsq.client.configs;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.dcc.client.ConfigClient;
import com.youzan.dcc.client.ConfigClientBuilder;
import com.youzan.dcc.client.entity.config.Config;
import com.youzan.dcc.client.entity.config.ConfigRequest;
import com.youzan.dcc.client.entity.config.interfaces.IResponseCallback;
import com.youzan.dcc.client.exceptions.ClientRuntimeException;
import com.youzan.dcc.client.exceptions.ConfigParserException;
import com.youzan.dcc.client.exceptions.InvalidConfigException;
import com.youzan.dcc.client.util.inetrfaces.ClientConfig;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.nsq.client.entity.Topic;
import com.youzan.nsq.client.entity.TopicTrace;
import com.youzan.util.LogUtil;
import com.youzan.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by lin on 16/9/20.
 */
public class TraceConfigAgent implements Closeable{
   private static final Logger logger = LoggerFactory.getLogger(TraceConfigAgent.class);
   //Config client to delegate subscribe configs on dcc for trace switches
   private ConfigClient configClient;
   //cached trace switches, config client subscribe job write updates into it and {@Link Traceability} implementation read
   //from it
   private Set<TopicTrace> tracedTopicSet = Collections.newSetFromMap(new ConcurrentHashMap<TopicTrace, Boolean>());
   //singleton instance&lock
   private static final Object LOCK = new Object();
   private static TraceConfigAgent INSTANCE = null;
   private static boolean punchLog = false;

   /**
    * ConfigAgent works on copying dcc config about topics trace to
    */
   private TraceConfigAgent(String[] urls, final ClientConfig config) throws IOException {
      this.configClient = ConfigClientBuilder.create()
              .setRemoteUrls(urls)
              .setClientConfig(config)
              .build();
   }

   public static TraceConfigAgent getInstance() {
      if(null == INSTANCE && NSQConfig.isConfigServerLookupOn()){
         synchronized(LOCK){
            if(null == INSTANCE && NSQConfig.isConfigServerLookupOn()){
                //read config client from static client config in NSQConfig, if either config or urls is specified, throws an exception
                //create config request
                try {
                    //As NSQConfig is invoked here, which means static variables like properties will be initialized before trace agent is invoked
                    INSTANCE = new TraceConfigAgent(NSQConfig.getUrls(), NSQConfig.getTraceAgentConfig());
                    INSTANCE.kickoff();
                } catch (IOException e) {
                    if(punchLog)
                        LogUtil.warn(logger, "Fail to read trace config content.");
                    else
                        logger.warn("Fail to read trace config content.", e);
                    INSTANCE = null;
                } catch (InvalidConfigException e) {
                    if(punchLog)
                        LogUtil.warn(logger, "Trace config " + e.getConfigInProblem() + " is invalid.");
                    else
                        logger.error("Trace config " + e.getConfigInProblem() + " is invalid.");
                    INSTANCE = null;
                } catch (ConfigParserException e) {
                    if(punchLog)
                        LogUtil.warn(logger, "Fail to parse config.");
                    else
                        logger.warn("Fail to parse config.", e);
                    INSTANCE = null;
                } catch (ClientRuntimeException e) {
                    if(punchLog)
                        LogUtil.warn(logger, "Could not configure trace config agent.");
                    else
                        logger.warn("Could not configure trace config agent.", e);
                    INSTANCE = null;
                } catch (Exception e) {
                    if(punchLog)
                        LogUtil.warn(logger, "Error in the initialization of trace config agent. Pls check dcc config.");
                    else
                        logger.warn("Error in the initialization of trace config agent. Pls check dcc config.", e);
                    INSTANCE = null;
                }finally {
                    punchLog = true;
                }
            }
         }
      }
      return INSTANCE;
   }

   /**
    * function to check if pass in topic has traced on
    * @param topic topic for trace lookup
    * @return true if trace switch is on, other wise false;
    */
   public boolean checkTraced(final Topic topic){
      TopicTrace topicTrace = new TopicTrace(topic.getTopicText());
      return this.tracedTopicSet.contains(
              topicTrace
      );
   }

   /**
    * start subscribe on pass in config request, update traced topic set once subscribe on changed returns configs
    */
   private void kickoff() throws ConfigParserException, IOException, InvalidConfigException {
       //build config request
       ConfigRequest configRequest = (ConfigRequest) ConfigRequest.create(this.configClient)
               .setApp(NSQConfig.NSQ_APP_VAL)
               .setKey(NSQConfig.NSQ_TOPIC_TRACE)
               .build();
      List<ConfigRequest> configs  = new ArrayList<>();
      configs.add(configRequest);
      TracedTopicsCallback callback = new TracedTopicsCallback();
      List<Config> firstConfigs = this.configClient.subscribe(callback, configs);
      if(null != firstConfigs && !firstConfigs.isEmpty()) {
          callback.updateTracedTopics(firstConfigs);
      }else{
           logger.warn("dcc remote returns nothing. Pls make sure config exist for config request: {}", configRequest.getContent());
      }
   }

   @Override
    /**
     * release resource allocated to config client
     */
    public void close() throws IOException {
        this.configClient.release();
        this.tracedTopicSet.clear();
    }

    private class TracedTopicsCallback implements IResponseCallback{

      void updateTracedTopics(final List<Config> updatedConfigs) throws InvalidConfigException, IOException {
         Config config = updatedConfigs.get(0);
         JsonNode root = SystemUtil.getObjectMapper().readTree(config.getContent());
         JsonNode val = root.get("value");
         assert val.isArray();
         for(JsonNode subVal : val){
            String topicName = subVal.get("key").asText();
            TopicTrace topicTrace = new TopicTrace(topicName);
            if(subVal.get("value").asText().equalsIgnoreCase("true")){
               //put subkey(topic name) into set
               tracedTopicSet.add(topicTrace);
                if(logger.isDebugEnabled())
                    logger.debug("TopicTrace {} Added.", topicTrace.toString());
            }else{
               //remove it from set
               tracedTopicSet.remove(topicTrace);
                if(logger.isDebugEnabled())
                    logger.debug("TopicTrace {} Removed.", topicTrace.toString());
            }
         }
      }

      @Override
      public void onChanged(List<Config> updatedConfigs) throws Exception {
          updateTracedTopics(updatedConfigs);
      }

      @Override
      public void onFailed(List<Config> configsInCache, Exception e) throws Exception {
         logger.error("Fail to subscribe to trace info", e);
      }
   }
}
