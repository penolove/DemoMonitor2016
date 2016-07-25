package com.loser.Hbase.training;

import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
/**
 * Hello world!
 *
 */
public class monitor_both 
{
	public static void main(String args[]) throws IOException
    {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum","InvPM30");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.cluster.distributed", "true");
        HTable table = new HTable(config, "streamsSample_both");
        HTable tableR = new HTable(config, "controlrec");
        
        Map recordmp=new HashMap();
        Scan scan = new Scan();

        //create a producer
        KafkaProducer<String, String> producer;
        Properties prop = new Properties();
		prop.put("bootstrap.servers", "172.16.20.80:9092,172.16.20.77:9092,172.16.20.78:9092,172.16.20.79:9092,172.16.20.81:9092");
		prop.put("acks", "0");
		prop.put("retries", "0");
		prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		prop.put("block.on.buffer.full", "true");
		producer = new KafkaProducer<>(prop);
        
        //variable zone        
        Long avgLatency=0L;
        long tempOutmax=0l;
        long tempInmax=0l;
        int idx2=0;
        int idx=0;
        int latecount=0;
        int ITlength=0;
        int OTlength=0;
        //rowkey
        int rowkey=0;
        
        
        while(true){
        	rowkey++;
	        scan.setTimeRange(tempInmax+1L, 1472260378164L);
	        scan.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timefirst"));
	        
	        ResultScanner scanner = table.getScanner(scan);
	        
	        idx=0;
	        //ArrayList<Long> inputList= new ArrayList<Long>() ;
	        for (Result result = scanner.next(); result != null; result = scanner.next()){
	        	idx++;
	        	 recordmp.put(Bytes.toString(result.getRow()),result.raw()[0].getTimestamp());
	        	if(tempInmax<result.raw()[0].getTimestamp()){
	        		tempInmax=result.raw()[0].getTimestamp();
	        	}
	        	//System.out.println(Bytes.toString(result.getRow()));
	        }
	        ITlength=idx;
	        
	
	        scanner.close();
	        Scan scan2 = new Scan();
	        scan2.setTimeRange(tempOutmax+1L, 1472260378164L);
	        scan2.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timeend"));
	        
	        idx2=0;
	        latecount=0;
	        scanner = table.getScanner(scan2);
	        
	        avgLatency=0L;
	        for (Result result = scanner.next(); result != null; result = scanner.next()){
        		if(tempOutmax<result.raw()[0].getTimestamp()){
        			tempOutmax=result.raw()[0].getTimestamp();
        		}
        		
	        	idx2++;
	        	try{
	        		avgLatency-=(Long) recordmp.get(Bytes.toString(result.getRow()))-(Long) result.raw()[0].getTimestamp();	
	        		latecount++;
	        		recordmp.remove(Bytes.toString(result.getRow()));
	        	}catch(Exception ex){
	        		continue;
	        	}
	        }
	        //System.out.printf("Input : %d ,Onput : %d ,Latency : %s \n",ITlength,OTlength, avgLatency.toString());
	        if(latecount==0){
	        	avgLatency=0L;
	        }else{
	        	avgLatency/=latecount;
	        }
	        
	        
	        OTlength=idx2;
	        
	        scanner.close();
	        if(rowkey>1){
		    System.out.printf("Input : %d k,Onput : %d k,Latency : %s ms\n",ITlength/3,OTlength/3, avgLatency.toString());
	            producer.send(new ProducerRecord<String, String>("RecordsInfo", Integer.toString(ITlength/3)+","+Integer.toString(OTlength/3)+","+avgLatency.toString()));
	            
	            Put p = new Put(Bytes.toBytes("IF"+Long.toString(rowkey)));
	            p.add(Bytes.toBytes("all"),Bytes.toBytes("timefirst"), Bytes.toBytes(Integer.toString(ITlength/3)));
	            tableR.put(p);
	        }
	        try{
	        	Thread.sleep(3000);
	        }catch(Exception e){
	        	System.out.println(e);
	        }
	     }
    }
}
