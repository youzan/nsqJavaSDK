## 2.3 final release version is 2.3.20170110-RELEASE
### 2.3M1
+ Apply ConfigAccessAgent to integrate lookup address and topic trace access to DCC.[#48](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/48)
+ Fluent API in NSQConfig [#49](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/49) and new NSQConfig constructor[#50](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/50)
+ Bug Fixes:
    - UnsupportedOperationException in remove Array.asList() temp list in update lookup address.[#25](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/25)
    - Remove SO_TIMEOUT in netty bootstrap config.[#29](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/29)
    
### 2.3M2
+ Topic partition refactor, user allow to specify partition sharding ID to specify partition in NSQd.[#51](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/51)
+ Connection pool size property in NSQConfig. New property for specifying connection pool size in producer.[#52](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/52)
+ Message meta data checker in consumer, in subscribe order mode.[#53](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/53)
+ Merge partition node and producers node in lookup response.[#55](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/55)

### 2.3M3
+ LookupAddressUpdate refactor for migration control config.[#57](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/57)
+ nsq meta data support in lookup API.[#57](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/57)
+ DCC sdk and nsq sdk wrapper.[#57](http://gitlab.qima-inc.com/paas/nsq-client-java/merge_requests/57)

## 2.3.20170117-RELEASE
Publish process fails without retry in three specified situations. Bug in lookup combination policy fixed.
### Features:
+ exceptions in publish process.[#44](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/44)
### Fixes:
+ curlookup reference is not assigned to NSQLookupd.[#43](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/43)

## 2.3.20170214-RELEASE
"dev"&"pre" env add for DCC in configClient.properties, and several bug fixes. In client, new API for producer publish(String, final Topic, Object), and
partition ID could be specified via Topic#setPartitionId(int).
### Features:
+ dev env appended in nested configClient.properties.[#47](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/47)
+ publish API with shardingID as parameter.[#49](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/49)
+ decrease delay for simple client to maintain data node. [#51](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/51)
+ setPartitionId API in Topic class. [#52]((http://gitlab.qima-inc.com/paas/nsq-client-java/issues/52))
+ fast recovery ability from net work partition. [#54](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/54)
### Fixes:
+ synchronization in boostrap map in producer connection pool factory.[#46](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/46)
+ nsq.sdk.env not work when set as system property.[#48](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/48)
+ ProducerNotFoundException when code 4XX from lookup.[#50](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/50) 

## 2.3.20170216-RELEASE
Note worthy:
Bug fixes release for closed NSQConnection clean up issue.
Error log for publish exception handle is truncated if message content is too large.
Default connection size capacity increased to 30.
Producer retry interval base is configurable, default base if modified to 100 milliseconds.
### Features:
+ truncate message output when exception raised in publish.[#55](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/55)
+ increase default connection size capacity to 30.[#57](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/57)
+ producer retry interval configurable.[#58](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/58)
### Fixes:
+ closed NSQConnection is returned to producer connection pool, without invalidated.[#56](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/56)

##2.3.20170309-RELEASE
Note worthy:
Policy for message ACK in autoFinish mode is modified, when one message exceeds retry limitation, in NON Sub order mode,
consumer will publish message to the NSQ queue end, and ACK that message which fail to consume in max retry times.

Bug fix in SDK consumer. When consumer subscribes:
   1. to multi-partitions total number is larger than NSQd nodes;
   2. to partition has more than one replica;
### Features:
+ max retry in consumer.[#62](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/62)
### Fixes:
+ SDK connection NOT recover when leader NSQd node switch.[#64](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/64)

##2.3.20170406-RELEASE
Note worthy:
Through it is a minor version release, a change is applied in the initialization of NSQ Client. That means if user upgrades from previous 2.3 release directly,
client fails at startup. In this release, lookupd address source(DCC or lookupd addresses) specification by user is required. One uses NSQConfig.setLookupAddresses()
to specify lookupd address source, or specify config properties which contains keys/values for config access agent only.

### Features:
+ improving client startup cost.[#68](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/68)
+ read dcc url&env from lookup address API.[#63](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/63)

### Fixes:
+ Existing SeedLookupConfig is always replaced by new.[#69](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/69)

##2.3.20170424-RELEASE
Note worthy:
jackson within NSQ SDK upgrade to 2.7.9.1. due to [deserializer security vulnerability](https://github.com/FasterXML/jackson-databind/issues/1599)
, also exception raised by missed topic control congfig needs to be thrown directly, without retry in producer end.

### Fixes:
+ Uncaught exceptions in producer publish.[#70](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/70)
+ Upgrade jackson to 2.7.9.1.[#71](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/71)

#### fix patch 20170509
+ regression in trace log.[#73](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/73)

##2.3.20170606-RELEASE
Note worthy:
Fix connection leak in producer in ordered message production scenario. Performance debug log in producer and message handle.

### Feature:
+ performance log in producer&consumer[#72](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/72)

### Fixes:
+ connection leak in producer in ordered topic production[#76](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/76)

## 2.4-RELEASE
Note worthy:

### Features:
+ performance log in consumer&producer.[#72](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/72)
+ min idle nsqd connection for producer.[#74](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/74)
+ condition lock in getPartitionNodes.[#79](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/79)
+ rdy dynamic adjustment according to consumption status.[#80](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/80)

# Fixes:
+ connection leak in producer in ordered topic production.[#76](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/76)
+ compressed message body should not treat as string.[#81](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/81)

## 2.4.0.1-RELEASE
Note worthy:
Validation removed when producer intends to PUB_EXT a message to nsq topic, regardless whether it is extend support.
Fix a NPE risk, in the eviction(close) of producer idle connection, while heart beat validation arrives.
Change type of schedule executor in ConnectionManager to optimize thread number.

#2.4.0.2-RELEASE
Note worthy:
Print message extension json header and desired tag in error log during publish.
 
#2.4.0.3-RELEASE
Note worthy:
Remove app-test.properties file when package.

#2.4.1-RELEASE
Note worthy:

fix:
publish problem in producers at start up when lookup address and dcc remove control both applied.
NSQExtNotSupportedException added for new error in youzan nsqd.

feature: filter to skip message according to extension header key specified in NSQConfig for consumer.

#2.4.1.1-RELEASE
Note worthy:
fix remove nested logback.xml imported in 2.4.1-RELEASE.
Add ExplicitRequeueException in message consumption to depress log. 

#2.4.1.3-RELEASE
Note worthy:
* Producer: block connection borrowing when pool exhausted. Producer maintain a pool of nsqd connections per nsqd partition
address. When concurrent # exceeds connection pool size, publish process fails with pool exhausted. In current release, 
connection borrowing is blocked for a specified timeout(default 200ms) when pool exhausted.

#2.4.1.4-RELEASE
Note worthy:
* Consumer: 
  * type of counters including success, received, queue4Consume, finished, skipped improved to long;
  * remove RdyUpdatePolicy, rdy update are related to subscribe error received from nsqd. 

#2.4.1.5-RELEASE
Note worthy:
fix:
  * retired lookup address should be removed from SDK[#26](https://github.com/youzan/nsqJavaSDK/issues/26)
  * closed connection should be removed from connection manager[#26](https://github.com/youzan/nsqJavaSDK/issues/26)
  
#2.4.1.6-RELEASE
Note worthy:
fix:
  * consumer event group not shutdown[#29](https://github.com/youzan/nsqJavaSDK/issues/29)
  
#2.4.1.7-RELEASE
Note worthy:
fix
 * error list lookup error after continuous 3 times[#30](https://github.com/youzan/nsqJavaSDK/issues/30)