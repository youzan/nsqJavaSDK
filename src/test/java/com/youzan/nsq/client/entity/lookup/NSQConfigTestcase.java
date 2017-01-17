package com.youzan.nsq.client.entity.lookup;

import com.youzan.nsq.client.configs.ConfigAccessAgent;
import com.youzan.nsq.client.configs.DCCConfigAccessAgent;
import com.youzan.nsq.client.entity.NSQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by lin on 16/12/21.
 */
public class NSQConfigTestcase {

    private final static Logger logger = LoggerFactory.getLogger(NSQConfigTestcase.class);
    private Properties props = new Properties();

    @BeforeClass
    public void init() throws IOException {
        logger.info("init of [NSQConfigTestcase].");
        logger.info("Initialize ConfigAccessAgent from system specified config.");
        System.setProperty("nsq.sdk.configFilePath", "src/main/resources/configClient.properties");
        logger.info("init of [NSQConfigTestcase] ends.");
    }

    @Test
    public void testSetDCCUrlsInNSQConfig() {
        String dumyUrl = "http://invalid.dcc.url:1234";
        ConfigAccessAgent.setConfigAccessRemotes(dumyUrl);
        NSQConfig config = new NSQConfig();
        DCCConfigAccessAgent agent = (DCCConfigAccessAgent) ConfigAccessAgent.getInstance();
        Assert.assertEquals(dumyUrl, DCCConfigAccessAgent.getUrls()[0]);
    }

    @Test
    public void testNSQConfig() {
        NSQConfig config = new NSQConfig();
        config
                /*
                开启／关闭顺序消费模式。默认状态下顺序消费模式为关闭状态。
                */
                .setOrdered(true)
                /*
                用于覆盖DCC返回的lookup address，配置为true时，ConfigAccessAgent将不会访问DCC，SDK
                使用｛@link NSQConfig#setLookupAddresses(String)｝传入的地址。
                */
                .setUserSpecifiedLookupAddress(true)
                /*
                指定Seed lookup address。UserSpecifiedLookupAddress 未指定或指定为{@link Boolean#FALSE}时
                传入地址将被忽略。在UserSpecifiedLookupAddress 为{@link Boolean#TRUE} 时有效。
                */
                .setLookupAddresses("sqs-qa.s.qima-inc.com:4161")
                /*
                指定Consumer消费的Channel，该配置对producer无效
                 */
                .setConsumerName("BaseConsumer")
                /*
                NSQd connection pool 的容量设置，默认值为10。
                 */
                .setConnectionPoolSize(10)
                /*
                开启／关闭Rdy慢启动。默认值为{@Boolean#TRUE}
                 */
                .setConsumerSlowStart(false)
                /*
                NSQ消息超时配置, 默认值为60秒
                 */
                .setMsgTimeoutInMillisecond(10000)
                /*
                NSQ output_buffer_size 配置项，可选配置
                 */
                .setOutputBufferSize(10000)
                /*
                NSQ output_buffer_timeout 配置项，可选配置
                 */
                .setOutputBufferTimeoutInMillisecond(10000)
                /*
                SDK 同步操作时的等到超时设置，可选配置
                 */
                .setQueryTimeoutInMillisecond(30000)
                /*
                Consumer RDY最大值设置
                 */
                .setRdy(3)
                /*
                用户消息处理线程池容量配置项
                 */
                .setThreadPoolSize4IO(Runtime.getRuntime().availableProcessors());
    }

    @AfterMethod
    public void release(){
        System.clearProperty("nsq.sdk.configFilePath");
    }
}
