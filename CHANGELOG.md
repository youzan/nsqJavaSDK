## 2.2.20170406-RELEASE
Note worthy:

Minor update for porting back bugs.
 
### Features:
+ improving client start cost.[#3](https://github.com/youzan/nsqJavaSDK/issues/3)

### Fixes:
+ NSQConnection should be invalidated when it is closed in producer exception handler.[#2](https://github.com/youzan/nsqJavaSDK/issues/2)
+ NPE in connection pool when finishing a message.[#1](https://github.com/youzan/nsqJavaSDK/issues/1)

## 2.2.20170424-RELEASE
Note worthy:
jackson within NSQSDK is upgrade to 2.7.9.1. due to [deserializer security vulnerability](https://github.com/FasterXML/jackson-databind/issues/1599)
### Fixes:
+ Upgrade jackson to 2.7.9.1.[#4](https://github.com/youzan/nsqJavaSDK/issues/4)