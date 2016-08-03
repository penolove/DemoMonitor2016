# DemoMonitor2016

mvn clean package

SparkHbaseTrafficMonitor
```
trace the Hbase of Spark (Hbase: streamsSample_spark)
RecordsInfo is for UI-Demo (kafka topic : RecordsSpark)
used to measure speed(Hbase : controlrec)
usage:

./target/DemoMonitor STM
```

InfosphereHbaseTrafficMonitor
```
trace the Hbase of Spark (Hbase: streamsSample_books)
RecordsInfo is for UI-Demo (kafka topic : RecordsInfo)
used to measure speed(Hbase : controlrec)
usage:
./target/DemoMonitor ITM
```

BothHbaseTrafficMonitor
```
trace the Hbase of Both (Hbase: streamsSample_books,streamsSample_spark)
RecordsInfo is for UI-Demo (kafka topic : RecordsInfo)
used to measure speed(Hbase : controlrec)
usage:
./target/DemoMonitor both
```

SplitFlowComsumer
```
switch flow rate:v default is 140k/s
if v <140 =>Infosphere ,if v>=140 => spark streaming
Infosphere      with kafak topic :7party1-Topicarray[0]
spark streaming with kafak topic :16party-Topicarray[0]
used to measure speed(Hbase : controlrec)
usage:
./target/DemoMonitor SFC [partitioner] [# of partition] [switch_flow_rate]
ex.
./target/DemoMonitor SFC 3 2
./target/DemoMonitor SFC 3 1

```

SplitFlowComsumerwithProb
```
Switch Flows using prob
usage:
./target/DemoMonitor SFC [partitioner] [# of partition] [switch_rate]
ex.
./target/DemoMonitor SFCWP 3 2
./target/DemoMonitor SFCWP 3 1

```


