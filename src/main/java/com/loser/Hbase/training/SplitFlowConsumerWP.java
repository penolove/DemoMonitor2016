package com.loser.Hbase.training;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.hadoop.hbase.client.Put;
import java.io.IOException;

public class SplitFlowConsumerWP {
    public static void main(String args[]) throws IOException
    {
        
        int partitioner=1;
        int No=1;
        double switch_rate=0;
        if(args.length>=3){
            partitioner=Integer.parseInt(args[1]);
            No=Integer.parseInt(args[2]);
        }

        if(args.length>=4){
            switch_rate=Double.parseDouble(args[3]);
        }

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","InvPM30");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.cluster.distributed", "true");
        HTable table = new HTable(config, "controlrec");
        //if split => SPtable : streamsSample_spark
        //         => INable : streamsSample_books
        //if mixture => SPtable : streamsSample_both
        //           => INable : streamsSample_both
        HTable SPtable = new HTable(config, "streamsSample_both");
        HTable INable = new HTable(config, "streamsSample_both");
        Scan scan = new Scan();
        
        
        //create a producer
        KafkaProducer<String, String> producer;
        Properties prop_producer = new Properties();
        prop_producer.put("bootstrap.servers", "172.16.20.80:9092,172.16.20.77:9092,172.16.20.78:9092,172.16.20.79:9092,172.16.20.81:9092");
        prop_producer.put("acks", "0");
        prop_producer.put("retries", "0");
        prop_producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop_producer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop_producer.put("block.on.buffer.full", "true");
        producer = new KafkaProducer<>(prop_producer);
        
        
        KafkaConsumer<String, String> consumer;
        Properties prop_consumer= new Properties();
        prop_consumer.put("bootstrap.servers","10.0.20.77:9092,10.0.20.78:9092,10.0.20.79:9092,10.0.20.80:9092,10.0.20.81:9092,10.0.20.83:9092");
        prop_consumer.put("group.id","test");
        prop_consumer.put("enable.auto.commit","true");
        prop_consumer.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop_consumer.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(prop_consumer);

        consumer.subscribe(Arrays.asList("SplitResource"));
        long timeouts=No;
        long timecounts=0;
        long modd=0;
        long timeout_back=0;
        double ranseed;
        double latency =(System.nanoTime() * 1e-9);
        double latency1 =0;
        String[] Topicarray={"7party1","16party"};
        
        int switchterm=0;
        int throughput_temp=0;
        String topictemp=Topicarray[0];
        
        
        
        //initial get newest Timestamp
        int idx=0;
        long tempInmax=0l;
        scan.setTimeRange(0, 1472260378164L);
        scan.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timefirst"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            
            if(tempInmax<result.raw()[0].getTimestamp()){
                tempInmax=result.raw()[0].getTimestamp();
            }
            
        }
        scanner.close();
        
        
        
        
        
        
        while (true) {
                // read records with a short timeout. If we time out, we don't really care.
                ConsumerRecords<String, String> records = consumer.poll(500);
            
                for (ConsumerRecord<String, String> record : records) {
                      timeouts+=partitioner;
                      timecounts++;
                      //System.out.printf("Got %d \n", timeouts );

                      producer.send(new ProducerRecord<String, String>(topictemp, Long.toString(timeouts)+","+record.value()));
                      
                      
                      if(timeouts%1000==0){
                              if(switchterm==0){
                                Put p = new Put(Bytes.toBytes(Long.toString(timeouts)));
                                p.add(Bytes.toBytes("all"),Bytes.toBytes("timefirst"), Bytes.toBytes(Long.toString(timeouts)));
                                INable.put(p);
                              }else{
                                Put p = new Put(Bytes.toBytes(Long.toString(timeouts)));
                                p.add(Bytes.toBytes("all"),Bytes.toBytes("timefirst"), Bytes.toBytes(Long.toString(timeouts)));
                                SPtable.put(p);
                              }

                              ranseed=Math.random();
                              if(ranseed>switch_rate){
                                  topictemp=Topicarray[1];
                                  switchterm=1;
                              }else{
                                  topictemp=Topicarray[0];
                                  switchterm=0;
                              }

                      }
                }
            
        }
        
    }


}
