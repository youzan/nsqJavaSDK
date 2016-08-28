# NSQ Ops

 * curl -X POST "http://127.0.0.1:4161/topic/create?topic=$$&partition_num=2&replicator=1&suggestload=0&syncdisk=1000"
 * curl -X GET "http://127.0.0.1:4161/lookup?topic=$$&access=w"
 
## New Topics
 
  * JavaTesting-Producer-Base
  * JavaTesting-ReQueue
  * JavaTesting-Finish


## TestNG

 * mvn clean test-compile failsafe:integration-test -PQA -Dfailsafe.suiteXmlFiles=src/test/resources/testng-stable-suite.xml -Dstable=true -Dhours=4