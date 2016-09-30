package com.loser.Hbase.training;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
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
        tableName = TableName.valueOf("Rec4Speed");
		Table speedTable = con.getTable(tableName); 
        Scan scan = new Scan();
        long tempInmax=System.currentTimeMillis();
        long temptmep=0;
        long acclatency=0;
        long avgacclatency=0;
        double Speed_temp=0;

        
        while(true){
	        scan.setTimeRange(tempInmax, 1502260378164L);
	        ResultScanner scanner = INable.getScanner(scan);
	        long latency=0;
	        int idx=0;
	        tempInmax=System.currentTimeMillis();
	        for (Result result = scanner.next(); result != null; result = scanner.next()){
	        	latency+=Long.parseLong(Bytes.toString(result.getValue(Bytes.toBytes("all"), Bytes.toBytes("timeend"))));
	        	idx++;
	        }
            Get theGet = new Get(Bytes.toBytes("Rowkey"));

            Result result =speedTable.get(theGet);
            Speed_temp=Double.parseDouble(Bytes.toString(result.value()));
	        if(idx>0){
		        acclatency+=latency;
		        avgacclatency+=latency/idx;
	        	System.out.printf("Input: %f, rows: %d, avg-latency : %d, acc-avglatency : %d , acc-latency: %d \n",Speed_temp,idx,latency/idx,avgacclatency,acclatency);
	        }else{
	        	System.out.printf("Input: %f, rows: %d, avg-latency : %d, acc-avglatency : %d , acc-latency: %d \n",Speed_temp,0,0,avgacclatency,acclatency);
	        }
	        
	        temptmep=5000-(System.currentTimeMillis()-tempInmax);
	        if(5000-temptmep>0){
	        	//System.out.printf("thread to sleep : %d\n",temptmep);
	        	Thread.sleep(temptmep);
	        }
	        
	        
        }
	}
}
