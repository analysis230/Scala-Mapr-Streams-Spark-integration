# Scala-Mapr-Streams-Spark-integration
Reads data from MapR streams and writes back into MapR streams as well

Commands to run :

Step 1: First compile the project on eclipse: Select project  -> Run As -> Maven Install

Step 2: use scp to copy the ms-sparkstreaming-1.0.jar to the mapr sandbox or cluster
        [Or you can copy the project to the cluster and package using maven, up to the user]
        
        given below are the commands used by me for my cluster

also use scp to copy the data sensor.csv file from the data folder to the cluster
put this file in a folder called data. The producer reads from this file to send messages.

scp  ms-sparkstreaming-1.0.jar user01@ipaddress:/user/user01/.
if you are using virtualbox:
scp -P 2222 ms-sparkstreaming-1.0.jar user01@127.0.0.1:/user/user01/.

Create the topic

maprcli stream create -path /user/user01/original -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/original -topic test

maprcli stream create -path /user/user01/pass -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/pass -topic test

maprcli stream create -path /user/user01/fail -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/fail -topic test

get info on the topic
maprcli stream info -path /user/user01/<stream name>


To run the MapR Streams Java producer and consumer:
go to the spark home installation directory:

run the command, bin/spark-submit --master local[2] --class com.streams.Scala_Consumer [path to your JAR file]
