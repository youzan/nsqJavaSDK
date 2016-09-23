package com.youzan.nsq.client.configs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.youzan.nsq.client.entity.TopicTraceAgent;
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

   private ConfigClient configClient;
   private static Object LOCK = new Object();
   private Set<TopicTraceAgent> tracedTopicSet = Collections.newSetFromMap(new ConcurrentHashMap<TopicTraceAgent, Boolean>());
   private ObjectMapper mapper = new ObjectMapper();

   //instance
   private static TraceConfigAgent INSTANCE = null;

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
      if(null == INSTANCE){
         synchronized(LOCK){
            if(null == INSTANCE){
                //read config client from static client config in NSQConfig, if either config or urls is specified, throws an exception
                //create config request
                try {
                    INSTANCE = new TraceConfigAgent(NSQConfig.getUrls(), NSQConfig.getTraceAgentConfig());
                    INSTANCE.kickoff();
                } catch (IOException e) {
                    logger.error("Fail to read trace config content.", e);
                    INSTANCE = null;
                } catch (InvalidConfigException e) {
                    logger.warn("Trace config " + e.getConfigInProblem() + " is invalid.", e);
                    INSTANCE = null;
                } catch (ConfigParserException e) {
                    logger.warn("Fail to parse config.", e);
                    INSTANCE = null;
                } catch (ClientRuntimeException e) {
                    logger.warn("Could not configure trace config agent.", e);
                    INSTANCE = null;
                }

            }
         }
      }
      return INSTANCE;
   }

   /**
    * function to check if pass in topic has traced on
    * @param topic
    * @return
    */
   public boolean checkTraced(final Topic topic){
      return this.tracedTopicSet.contains(new TopicTraceAgent(topic));
   }

   /**
    * start subscribe on pass in config request, update traced topic set once subscribe on changed returns configs
    */
   private void kickoff() throws ConfigParserException, IOException, InvalidConfigException {
       //build config request
       ConfigRequest configRequest = (ConfigRequest) ConfigRequest.create(this.configClient)
               //TODO: what is the value we finally need
               .setApp("nsq")
               .setKey("trace")
               .build();
      List<ConfigRequest> configs  = new ArrayList<>();
      configs.add(configRequest);
      TracedTopicsCallback callback = new TracedTopicsCallback();
      List<Config> firstConfigs = this.configClient.subscribe(callback, configs);
      callback.updateTracedTopics(firstConfigs);
   }

    @Override
    /**
     * release resource allocated to config client
     */
    public void close() throws IOException {
        this.configClient.release();
        this.tracedTopicSet.clear();
    }

    class TracedTopicsCallback implements IResponseCallback{

      public void updateTracedTopics(final List<Config> updatedConfigs) throws InvalidConfigException, IOException {
         Config config = updatedConfigs.get(0);
         JsonNode root = mapper.readTree(config.getContent());
         JsonNode val = root.get("value");
         assert val.isArray();
         for(JsonNode subVal : val){
            String topicName = subVal.get("key").asText();
            TopicTraceAgent topicTrace = new TopicTraceAgent(new Topic(topicName));
            if(subVal.get("value").asText().equalsIgnoreCase("true")){
               //put subkey(topic name) into set
               tracedTopicSet.add(topicTrace);
            }else{
               //remove it from set
               tracedTopicSet.remove(topicTrace);
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
