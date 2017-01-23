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

## 2.3.20170118.085718-24
dev&pre env add for DCC in configClient.properties, and several bug fixes.
### Features:
+ dev env appended in nested configClient.properties.[#47](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/47)
+ publish API with shardingID as parameter.[#49](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/49)
### Fixes:
+ synchronization in boostrap map in producer connection pool factory.[#46](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/46)
+ nsq.sdk.env not work when set as system property.[#48](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/48)
+ ProducerNotFoundException when code 4XX from lookup.[#50](http://gitlab.qima-inc.com/paas/nsq-client-java/issues/50) 