import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/*
 *This program addes data to a HBase table, the column qualifiers are made when entering the data 
 *
 *The program requires an HBase configuration and the location of the hbase-site.xml file and the location of the master 
 *
 *The values are converted to byte arrays and the "put" command is used to add the data to the data base  
 *
 *
 *This program was run in eclipse as it is not a map reduce program. Although it does need access to all the HBase jar files
 *There are about 15 jar files needed to run this program, this is why I ran it from eclipse because I had all the jar files 
 *already linked to the program
 *
 *Jars required to run and compile:
 *usr/local/hbase-0.98.7-hadoop2/lib/hbase-common-0.98.7-hadoop2.jar
 *commons-logging-1.1.1.jar
 *log4j-1.2.17.jar
 *zookeeper-3.4.6.jar
 *hadoop-common-2.2.0.jar
 *commons-configuration-1.6.jar
 *commons-lang-2.6.jar
 *slf4j-log4j12-1.6.4.jar
 *hbase-client-0.98.7-hadoop2.jar
 *hadoop-client-2.2.0.jar
 *hbase-protocol-0.98.7-hadoop2.jar
 *protobuf-java-2.5.0.jar
 *guava-12.0.1.jar
 *hadoop-auth-2.2.0.jar
 *slf4j-api-1.6.4.jar
 *htrace-core-2.04.jar
 *netty-3.6.6.Final.jar
 *commons-codec-1.7.jar
 *
 *run Command: run as a normal java program: java addData
 *
 *List of jar HBase jar files needed to run 
 *
 */

public class addData {

	public static void main (String[] args){
		Configuration conf = HBaseConfiguration.create(); 
		conf.addResource("/usr/local/hbase-0.98.7-hadoop2/conf/hbase-site.xml");
		conf.set("hbase.master","localhost:60000");
		
		try {
			HTable htable = new HTable(conf, "SuperCars");
			String car1 = "One77";
			byte[] carOne = Bytes.toBytes(car1);
			Put p = new Put(carOne);
			
			byte[] columnFamily = Bytes.toBytes("CarStats");
			byte[] columnQ_Make = Bytes.toBytes("make");
			byte[] columnQ_Year = Bytes.toBytes("year");
			byte[] columnQ_BHP  = Bytes.toBytes("BHP");
			
			byte[] value_Make1 = Bytes.toBytes("Aston Martin"); //One77
			byte[] value_Make2 = Bytes.toBytes("Koenigsegg"); //one:1
			byte[] value_Make3 = Bytes.toBytes("Nissan"); //GTR
			
			byte[] value_Year1 = Bytes.toBytes("2009");
			byte[] value_Year2 = Bytes.toBytes("2014");
			byte[] value_Year3 = Bytes.toBytes("2013");
			
			byte[] value_BHP1 = Bytes.toBytes("750");
			byte[] value_BHP2 = Bytes.toBytes("1341");
			byte[] value_BHP3 = Bytes.toBytes("600");
			
			p.add(columnFamily, columnQ_Make, value_Make1);
			p.add(columnFamily, columnQ_Year, value_Year1);
			p.add(columnFamily, columnQ_BHP, value_BHP1);
			htable.put(p);
			
			byte[] carTwo = Bytes.toBytes("one:1");
			p = new Put(carTwo);
			p.add(columnFamily, columnQ_Make, value_Make2);
			p.add(columnFamily, columnQ_Year, value_Year2);
			p.add(columnFamily, columnQ_BHP, value_BHP2);
			htable.put(p);
			
			byte[] carThree = Bytes.toBytes("GTR");
			p = new Put(carThree);
			p.add(columnFamily, columnQ_Make, value_Make3);
			p.add(columnFamily, columnQ_Year, value_Year3);
			p.add(columnFamily, columnQ_BHP, value_BHP3);
			htable.put(p);
			
			htable.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
