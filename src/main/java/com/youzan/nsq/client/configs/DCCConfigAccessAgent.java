package com.youzan.nsq.client.configs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.youzan.dcc.client.ConfigClient;
import com.youzan.dcc.client.ConfigClientBuilder;
import com.youzan.dcc.client.entity.config.Config;
import com.youzan.dcc.client.entity.config.ConfigRequest;
import com.youzan.dcc.client.entity.config.interfaces.IResponseCallback;
import com.youzan.dcc.client.exceptions.ConfigParserException;
import com.youzan.dcc.client.exceptions.InvalidConfigException;
import com.youzan.dcc.client.util.inetrfaces.ClientConfig;
import com.youzan.nsq.client.entity.NSQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * DCCConfigAccessAgent, which send config request to configs remote with configs client configs in configClient.properties
 * Created by lin on 16/10/26.
 */
public class DCCConfigAccessAgent extends ConfigAccessAgent {
    private static final Logger logger = LoggerFactory.getLogger(DCCConfigAccessAgent.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    //app value configs client need to specify to fetch lookupd config from configs
    private static final String NSQ_APP_VAL_PRO = "nsq.app.val";
    //default value for app value
//    private static final String DEFAULT_NSQ_APP_VAL = "nsq";
//    public static String NSQ_APP_VAL = null;
    //property urls to configs remote
    private static final String NSQ_DCCCONFIG_URLS = "nsq.configs.%s.urls";
    //property of backup file path
    private static final String NSQ_DCCCONFIG_BACKUP_PATH = "nsq.dcc.backupPath";

    //configs client configs values
    private static ConfigClient dccClient;
    private static String[] urls;
    private static String backupPath;
    private static String env;

    static {
        initDCCConfig();
    }

    public DCCConfigAccessAgent() throws IOException {
        ClientConfig dccConfig = new ClientConfig();
        //setup client config
        DCCConfigAccessAgent.dccClient = ConfigClientBuilder.create()
                .setRemoteUrls(urls)
                .setBackupFilePath(backupPath)
                .setConfigEnvironment(env)
                .setClientConfig(dccConfig)
                .build();
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
                JsonNode node = mapper.readTree(config.getContent());
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
    public SortedMap<String, String> handleSubscribe(String domain, final String[] keys, final IConfigAccessCallback callback) {
        if (null == domain || null == keys || keys.length == 0)
            return null;
        List<ConfigRequest> requests = new ArrayList<>();
        //create config requests out of pass in domain(app) and keys(keys)
        for (String key : keys) {
            ConfigRequest request = null;
            try {
                request = (ConfigRequest) ConfigRequest.create(dccClient)
                        .setApp(domain)
                        .setKey(key)
                        .build();
            } catch (ConfigParserException e) {
                logger.warn("Fail to parse config. {}", e.getContentInProblem(), e);
            }
            if (null != request)
                requests.add(request);
        }

        //subscribe
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
    }

    @Override
    protected void kickoff() {
        //no need to kick off.
    }

    @Override
    public void close() {
        //TODO: close configs client
    }

    /**
     * function to initialize configs lookup properties, function tries to file something to read properties from in
     * following order, default config file name is "clientConfig.properties":
     * 1. try search default config file in classpath;
     * 2. If #1 fails, try get config file path from system properties, the try loading properties from that path
     */
    private static void initDCCConfig() {
        InputStream is = null;
        try {
            is = NSQConfig.loadClientConfigInputStream();
            if (null != is) {
                initClientConfig(is);
            } else {
                logger.warn("Could not load properties for config server access configuration. Make sure User define a valid location of configClient.properties.");
                throw new RuntimeException();
            }
        } catch (FileNotFoundException configNotFoundE) {
            logger.warn("Config properties for nsq sdk to configs not found. Make sure properties file located under {} system property", NSQDCCCONFIGPRO);
            throw new RuntimeException(configNotFoundE);
        } catch (IOException IOE) {
            logger.error("Could not load properties from nsq config properties to initialize DCCConfigAccessAgent.");
            throw new RuntimeException(IOE);
        } finally {
            try {
                if(null != is)
                is.close();
            } catch (IOException e) {
                //swallow it
            }
        }
    }

    /**
     * initialize config client properties.
     *
     * @param is
     * @throws IOException
     */
    private static void initClientConfig(final InputStream is) throws IOException {
        Properties props = new Properties();
        props.load(is);

        //1.fixed properties initialization
        //1.1 config app
//        String app = props.getProperty(NSQ_APP_VAL_PRO);
//        if (null != app)
//            NSQ_APP_VAL = app;
//        else
//            NSQ_APP_VAL = DEFAULT_NSQ_APP_VAL;
//        logger.info("{}:{}", NSQ_APP_VAL_PRO, NSQ_APP_VAL);

        //1.2 config client backup file
        backupPath = props.getProperty(NSQ_DCCCONFIG_BACKUP_PATH);
        assert null != backupPath;
        logger.info("configs backup path: {}", backupPath);

        //1.3 nsq sdk env, which is also the env of nsq sdk
        env = NSQConfig.getSDKEnv();
        assert null != env;

        //1.4 config server urls, initialized based on sdk env
        String urlsKey = String.format(NSQ_DCCCONFIG_URLS, env);
        urls = props.getProperty(urlsKey)
                .split(",");
        assert null != urls;

    }
}
