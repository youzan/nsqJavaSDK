package com.youzan.nsq.client.configs;

import com.fasterxml.jackson.databind.JsonNode;
import com.youzan.dcc.client.ConfigClient;
import com.youzan.dcc.client.ConfigClientBuilder;
import com.youzan.dcc.client.entity.config.Config;
import com.youzan.dcc.client.entity.config.ConfigRequest;
import com.youzan.dcc.client.entity.config.interfaces.IResponseCallback;
import com.youzan.dcc.client.exceptions.ConfigParserException;
import com.youzan.dcc.client.exceptions.InvalidConfigException;
import com.youzan.dcc.client.util.inetrfaces.ClientConfig;
import com.youzan.nsq.client.entity.NSQConfig;
import com.youzan.util.SystemUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * DCCConfigAccessAgent, which send config request to configs remote with configs client configs in configClient.properties
 * Created by lin on 16/10/26.
 */
public class DCCConfigAccessAgent extends ConfigAccessAgent {
    private static final Logger logger = LoggerFactory.getLogger(DCCConfigAccessAgent.class);
    //app value configs client need to specify to fetch lookupd config from configs
    //property urls to configs remote
    private static final String NSQ_DCCCONFIG_URLS = "nsq.dcc.%s.urls";
    //property of backup file path
    private static final String NSQ_DCCCONFIG_BACKUP_PATH = "nsq.dcc.backupPath";

    //configs client configs values
    private static ConfigClient dccClient;
    private static String[] urls;
    private static String backupPath;
    private static String env;

    public DCCConfigAccessAgent() throws IOException {
        initClientConfig();
        ClientConfig dccConfig = new ClientConfig();
        //setup client config
        if(null == urls || null == env){
            logger.info("ConfigAccessAgent is running in local mode.");
            setConnected(false);
            return;
        }

        DCCConfigAccessAgent.dccClient = ConfigClientBuilder.create()
                .setRemoteUrls(urls)
                .setBackupFilePath(backupPath)
                .setConfigEnvironment(env)
                .setClientConfig(dccConfig)
                .build();
    }

    public static String[] getUrls() {
        return DCCConfigAccessAgent.urls;
    }

    public static String getBackupPath() {
        return DCCConfigAccessAgent.backupPath;
    }

    public static String getEnv() {
        return DCCConfigAccessAgent.env;
    }

    /**
     * simply extract values of config in to String array
     *
     * @param list updated configs from configs subscribe
     * @return {@link SortedMap presenting mapping between keys and config values}
     */
    private static SortedMap<String, String> extractValues(final List<Config> list) {
        SortedMap<String, String> configMap = new TreeMap<>();
        for (Config config : list) {
            try {
                JsonNode node = SystemUtil.getObjectMapper().readTree(config.getContent());
                JsonNode valueNode = node.get("value");
                if (valueNode.isArray()) {
                    //has subkeys
                    for (JsonNode val : valueNode) {
                        configMap.put(val.get("key").asText(),
                                val.get("value").asText());
                    }
                } else {
                    //simple value
                    configMap.put(valueNode.get("key").asText(),
                            valueNode.get("value").asText());
                }
            } catch (IOException | InvalidConfigException e) {
                logger.error("Invalid config content in config list.");
            }
        }
        return configMap;
    }

    @Override
    public SortedMap<String, String> handleSubscribe(AbstractConfigAccessDomain domain, AbstractConfigAccessKey[] keys, final IConfigAccessCallback callback) {
        if(!isConnected()) {
            logger.warn("DCCConfigAccessAgent is not connected. Subscribe returns null.");
            return null;
        }

        if (keys.length > 1)
            throw new IllegalArgumentException("DCCConfigAccessAgent does not accept more than one key(consumer or producer).");
        if (null == domain || keys.length == 0)
            return null;
        List<ConfigRequest> requests = new ArrayList<>();
        //create config requests out of pass in domain(app) and keys(keys)
        for (AbstractConfigAccessKey key : keys) {
            ConfigRequest request = null;
            try {
                request = (ConfigRequest) ConfigRequest.create(dccClient)
                        .setApp(domain.toDomain())
                        .setKey(key.toKey())
                        .build();
            } catch (ConfigParserException e) {
                logger.warn("Fail to parse config. {}", e.getContentInProblem(), e);
            }
            if (null != request)
                requests.add(request);
        }

        //subscribe
        long start = System.currentTimeMillis();
        try {
            List<Config> firstList = dccClient.subscribe(new IResponseCallback() {
                @Override
                public void onChanged(List<Config> list) throws Exception {
                    SortedMap<String, String> map = extractValues(list);
                    callback.process(map);
                }

                @Override
                public void onFailed(List<Config> list, Exception e) throws Exception {
                    SortedMap<String, String> map = extractValues(list);
                    callback.fallback(map, e);
                }
            }, requests);
            return extractValues(firstList);
        }finally {
            if(logger.isDebugEnabled())
                logger.debug("Time eclapse: {} millisec in getting subscribe response from config access remote.", System.currentTimeMillis() - start);
        }
    }

    @Override
    protected void kickoff() {
        logger.info("DCCConfigAccessAgent kick off.");
        //no need to kick off.
    }

    @Override
    public void close() {
        if(this.isConnected())
            this.dccClient.release();
    }

    @Override
    public String metadata() {
        StringBuilder sb = new StringBuilder(DCCConfigAccessAgent.class.getName() + "\n");
        String urlStr = "";
        for(String aUrl:urls)
            urlStr += aUrl + ";";
        sb.append("\turls: [").append(urlStr).append("]\n")
                .append("\tenv: [").append(env).append("]\n")
                .append("\tbackupPath: [").append(backupPath).append("]\n");
        return sb.toString();
    }

    /**
     * initialize config client properties.
     *
     * @throws IOException
     */
    private static void initClientConfig() throws IOException {

        //1.2 config client backup file
        backupPath = props.getProperty(NSQ_DCCCONFIG_BACKUP_PATH);

        String customizedBackup = ConfigAccessAgent.getConfigAccessAgentBackupPath();
        if(null != customizedBackup && !customizedBackup.isEmpty()){
            logger.info("Initialize backupPath with user specified value {}.", customizedBackup);
            backupPath = customizedBackup;
        }
        assert null != backupPath;
        logger.info("configs backup path: {}", backupPath);
        //write back to config access API
        ConfigAccessAgent.setConfigAccessAgentBackupPath(backupPath);

        //1.3 nsq sdk env, which is also the env of nsq sdk
        env = props.getProperty(NSQ_DCCCONFIG_ENV);
        String sysEnv = System.getProperty(NSQ_DCCCONFIG_ENV);
        if(null != sysEnv && !sysEnv.isEmpty()){
            logger.info("Initialize config access Env with system property value {}.", sysEnv);
            env = sysEnv;
        }
        String customizedEnv = ConfigAccessAgent.getEnv();
        if(null != customizedEnv && !customizedEnv.isEmpty()){
            logger.info("Initialize config access Env with user specified value {}.",customizedEnv);
            env = customizedEnv;
        }

        //pick env from NSQConfig
        String configEnv = NSQConfig.getConfigAccessEnv();
        if(null != configEnv && !configEnv.isEmpty()){
            logger.info("Initialize config access Env with NSQConfig setting value {}.", configEnv);
            env = configEnv;
        }
        //write back to config access API
        ConfigAccessAgent.setEnv(env);

        urls = ConfigAccessAgent.getConfigAccessRemotes();
        //1.4 config server urls, initialized based on sdk env
        String urlsKey = String.format(NSQ_DCCCONFIG_URLS, env);
        String urlsStr = props.getProperty(urlsKey);
        if(null != urlsStr) {
            String[] configUrls = urlsStr.split(",");
            if(null != configUrls && configUrls.length > 0) {
                logger.info("Initialize config access remote URLs with config properties {}.", configUrls);
                urls = configUrls;
            }
        }

        String[] configUrls = NSQConfig.getConfigAccessURLs();
        if(null != configUrls && configUrls.length > 0){
            logger.info("Initialize config access remote URLs with NSQConfig setting value {}.", configUrls);
            urls = configUrls;
        }
        //write back to config access API
        ConfigAccessAgent.setConfigAccessRemotes(urls);
    }
}
