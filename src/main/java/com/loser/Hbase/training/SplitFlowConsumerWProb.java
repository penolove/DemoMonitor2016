package com.loser.Hbase.training;
import java.util.*;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class SplitFlowConsumerWProb {
	private static Configuration conf;
	private static Connection con;
	static KafkaProducer<String, String> producer;
	static KafkaConsumer<String, String> consumer;

	static {
		/** Hadoop class, load and provide conf to client AP.
		 *                load from hbase-site.xml, hbase-default.xml 
		 *  使用HBaseConfiguration來建立configuration */
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "InvPM30"); // override ZooKeeper quorum address to point to a different cluster
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		/** Connection        : create instance only once per Ap, share it during runtime
		 *  ConnectionFactory : retrieve Connection instance, configured as per given config  
		 *  https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/Connection.html 
		 *  https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/ConnectionFactory.html
		 */
		 // 設定完成後需要連線去連接資料庫，我們使用ConnectionFactory來建立連線
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
        
        Properties prop_producer = new Properties();
        prop_producer.put("bootstrap.servers", "172.16.20.80:9092,172.16.20.77:9092,172.16.20.78:9092,172.16.20.79:9092,172.16.20.81:9092");
        prop_producer.put("acks", "0");
        prop_producer.put("retries", "0");
        prop_producer.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop_producer.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop_producer.put("block.on.buffer.full", "true");
        producer = new KafkaProducer<>(prop_producer);
		
		
        Properties prop_consumer= new Properties();
        prop_consumer.put("bootstrap.servers","10.0.20.77:9092,10.0.20.78:9092,10.0.20.79:9092,10.0.20.80:9092,10.0.20.81:9092,10.0.20.83:9092");
        prop_consumer.put("group.id","test");
        prop_consumer.put("enable.auto.commit","true");
        prop_consumer.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop_consumer.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(prop_consumer);
        consumer.subscribe(Arrays.asList("SplitResource"));
		
	}
		

	
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
    	
    	
    	//table for records
		TableName tableName = TableName.valueOf("controlrec");
		Table table = con.getTable(tableName); 
		//SPtable for spark
		tableName = TableName.valueOf("streamsSample_both");
		Table SPtable = con.getTable(tableName); 
		//INable for Infopshere
		tableName = TableName.valueOf("streamsSample_both");
		Table INable = con.getTable(tableName); 


		
		Scan scan = new Scan();
		 
		 
		double latency =(System.nanoTime() * 1e-9);
		long tempTimemax=(long) (System.nanoTime()*1e-6);
		
		//kafka Consumer
		ConsumerRecords<String, String> records ;
		String[] Topicarray={"7party1","16party"};
		

		
		//timeout to calculate latency 
        long timeouts=No;
        String topictemp="7party1";
        int switchterm=0;
        double ranseed;
		while (true) {
			records = consumer.poll(500);
			for (ConsumerRecord<String, String> record : records) {
				timeouts+=partitioner;
				producer.send(new ProducerRecord<String, String>(topictemp, Long.toString(timeouts)+","+record.value()));

                if(timeouts%1000==0){
                    if(switchterm==0){
                      Put put = new Put(Bytes.toBytes(Long.toString(timeouts)));
                      put.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timefirst"), Bytes.toBytes(Long.toString(timeouts)));
                      INable.put(put);
                    }else{
                      Put put = new Put(Bytes.toBytes(Long.toString(timeouts)));
                      put.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timefirst"), Bytes.toBytes(Long.toString(timeouts)));
                      SPtable.put(put);
                    }

                    //switch uisng random numbers
                    ranseed=Math.random();
                    if(ranseed>switch_rate){
                        topictemp=Topicarray[0];
                        switchterm=1;
                    }else{
                        topictemp=Topicarray[1];
                        switchterm=0;
                    }
                    if(System.nanoTime() * 1e-9>=latency+5){
                  	  System.out.println("5 seconds");
                  	  ResultScanner scanner = table.getScanner(scan);
                        scan.setTimeRange(tempTimemax+1, 1472260378164L);
                        scan.addColumn(Bytes.toBytes("all"), Bytes.toBytes("timefirst"));
                        int count=0;
                        int speed=0;
                        for (Result result = scanner.next(); result != null; result = scanner.next()){
                        	count++;
                            if(tempTimemax<result.raw()[0].getTimestamp()){
                          	  tempTimemax=result.raw()[0].getTimestamp();
                            }
                            //speed+=Bytes.toInt(result.value());
                            speed+=Integer.parseInt((Bytes.toString(result.value())));
                        }
                        System.out.printf("Got speed : %d \n",speed/count);
                        latency=System.nanoTime() * 1e-9;
                        switch_rate=1/(1+Math.exp(-0.1*(speed/count-130)));
                        System.out.printf("Got Prob : %.1f \n",switch_rate);
                    }
              }

			}
		}
    }
}
