# NSQ-Client-Java

[![GitHub release](https://img.shields.io/github/release/youzan/nsqJavaSDK.svg)](https://github.com/youzan/nsqJavaSDK/releases/latest)

## Server
* https://github.com/absolute8511/nsq
* Feature: strong consistent replication

## Guide
* It has compatibility for http://nsq.io and our secondary development.


## Usage
### Producer-Example

    @Test
    public void produceUsingSimpleProducer() throws NSQException {
    final NSQConfig config = new NSQConfig();
    config.setLookupAddresses(lookups);             // 必选项. 设置Lookupd(MetaData)集群(多)地址, 是以","分隔的字符串,就是说可以配置一个集群里的多个节点.
                                                    //        每个Lookup使用HTTP的端口. Address包含: IP + Port
    config.setConnectTimeoutInMillisecond(timeout); // 可选项. TCP Connect Timeout
    config.setThreadPoolSize4IO(threadPoolSize4IO); // 可选项. Thread Pool Size with Netty-IO. 建议值:CPU数目
     
    String topicName = "topic_one";
    String topicNameAnother = "topic_two";
    final Producer producer = new ProducerImplV2(config);
    producer.start();
 
    // Demo : business processing
    long success = 0L, total = 0L;
    final long end = System.currentTimeMillis() + 1 * 3600 * 1000L;
    while (System.currentTimeMillis() <= end) {
        try {
            total++;
            p.publish(randomString().getBytes(IOUtil.DEFAULT_CHARSET), topicName);
            success++;
 
            //publish message to another topic.
            total++;
            p.publish(randomString().getBytes(IOUtil.DEFAULT_CHARSET), topicNameAnother);
            success++;
        } catch (Exception e) { // 无论是Producer/Consumer,都要对应把最大的Exception处理下
            logger.error("Exception", e);
        }
    }
    // Client做下优雅的关闭,通常是在进程退出前做关闭
    p.close();                                   
    logger.info("Publish. Total: {} , Success: {}", total, success);
    }

### Consumer-Example

    @Test
    public void consumeUsingSimpleConsumer() throws NSQException {
    final NSQConfig config = new NSQConfig();
    config.setLookupAddresses(lookups);              // 必选项. 设置Lookupd(MetaData)集群(多)地址, 是以","分隔的字符串,就是说可以配置一个集群里的多个节点
                                                     //        每个Lookup使用HTTP的端口. Address包含: IP + Port
    config.setConsumerName("BaseConsumer");          // 必选项. 独立Consumer的标识,命名合法字符[.a-zA-Z0-9_-] . 实际等价于NSQ里Channel概念
     
    config.setConnectTimeoutInMillisecond(timeout);  // 可选项. TCP Connect Timeout
    config.setThreadPoolSize4IO(threadPoolSize4IO);  // 可选项. Thread Pool Size with Netty-IO. 建议值:1
    config.setRdy(rdy);                              // 可选项. 设置Consumer每次被NSQ-Server推过来的消息数量
     
 
     
    final AtomicInteger received = new AtomicInteger(0);
    final Consumer consumer = new ConsumerImplV2(config, new MessageHandler() {
        @Override
        public void process(NSQMessage message) { // Delegate it to SDK working thread-pool
            // 消息已经保证了送到Client并且已经传递给了Client
            received.incrementAndGet();
        }
    });
    consumer.setAutoFinish(true); // 可选项. 默认是在Handler里正常处理OK后 就AutoFinish
    consumer.subscribe(topics);   // 必选项. 可以多次调用
    consumer.start();
    // Client做下优雅的关闭,通常是在进程退出前做关闭
    consumer.close();             
    logger.info("Consumer received {} messages.", received.get());
    } 