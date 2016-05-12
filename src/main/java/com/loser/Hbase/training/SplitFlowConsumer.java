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

public class SplitFlowConsumer {
	public static void main(String args[]) throws IOException
    {
		
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","InvPM30");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.cluster.distributed", "true");
        HTable table = new HTable(config, "controlrec");
        HTable SPtable = new HTable(config, "streamsSample_spark");
        HTable INable = new HTable(config, "streamsSample_books");
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
		long timeouts=0;
		int modd=800000;
        double latency =0;
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
	                  timeouts++;
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

	                	  	 
		                  if(timeouts%modd==0){
			                  latency=(System.nanoTime() * 1e-9);
			                  System.out.printf("Got %d records, %.1f seconds, %f k-throughput\n", timeouts , latency-latency1,modd/1000/(latency-latency1));
			                  latency1 = latency;
			                  throughput_temp=0;
			                  idx=0;
			                  scan.setTimeRange(tempInmax+1, 1472260378164L);
			                  scan.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timefirst"));
			                  scanner = table.getScanner(scan);
			                  for (Result result = scanner.next(); result != null; result = scanner.next()){
			                  	idx++;
			                  	throughput_temp+=Integer.parseInt(Bytes.toString(result.value()));
			                  	if(tempInmax<result.raw()[0].getTimestamp()){
			                  		tempInmax=result.raw()[0].getTimestamp();
			                  	}
			                  }
			                  
			                  scanner.close();
			                  if(idx>0){
				                  if(throughput_temp/idx*2>130){
				                	  topictemp=Topicarray[1];
				                	  switchterm=1;
				                  }else{
				                	  topictemp=Topicarray[0];
				                	  switchterm=0;
				                  }
				                  System.out.println(throughput_temp/idx*2);
			                  }
			                  
		                  }
	                  }
	            }
	        
	    }
		
    }


}
