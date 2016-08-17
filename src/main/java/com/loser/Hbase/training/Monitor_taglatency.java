package com.loser.Hbase.training;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Monitor_taglatency {
	private static Configuration conf;
	private static Connection con;

	static{
		
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "InvPM30"); // override ZooKeeper quorum address to point to a different cluster
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		 // 設定完成後需要連線去連接資料庫，我們使用ConnectionFactory來建立連線
		try {
			con = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException, InterruptedException {
        TableName tableName = TableName.valueOf("streamsSample_taglatency");
		Table INable = con.getTable(tableName); 
        Scan scan = new Scan();
        long tempInmax=System.currentTimeMillis();
        long temptmep=0;
        
        
        while(true){
	        scan.setTimeRange(tempInmax, 1472260378164L);
	        ResultScanner scanner = INable.getScanner(scan);
	        int latency=0;
	        int idx=0;
	        for (Result result = scanner.next(); result != null; result = scanner.next()){
	        	latency+=Integer.parseInt(Bytes.toString(result.getValue(Bytes.toBytes("all"), Bytes.toBytes("timeend"))));
	        	idx++;
	        }
	        
	        if(idx>0){
	        	System.out.printf("rows: %d,avg-latency : %d\n",idx,latency/idx);
	        }else{
	        	System.out.printf("rows: %d,avg-latency : %d\n",0,0);
	        }
	        if(5000-(System.currentTimeMillis()-tempInmax)>0){
	        	temptmep=5000-(System.currentTimeMillis()-tempInmax);
	        	tempInmax=System.currentTimeMillis();
	        	Thread.sleep(temptmep);
	        }
	        
	        
        }
	}
}