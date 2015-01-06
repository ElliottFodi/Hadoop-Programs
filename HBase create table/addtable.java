import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/*
 *This programs adds a table to HBase with column families 
 *
 *The program requires a HBase configuration with the location of the 
 *hbase-site.xml and location of the master
 *
 *A table is created and a column family is added to the table
 *
 *In order to create the table in HBase an admin needs to be created 
 *Using the admin the create table command can be issued and then the connection closed
 *
 *Jars required to compile and run:
 *hbase-common-0.98.7-hadoop2.jar
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
 *run Command: run as a normal java program: java addtable
 */

public class addtable {

	public static void main(String[] args) {

		Configuration conf = HBaseConfiguration.create(); 
		conf.addResource("/usr/local/hbase-0.98.7-hadoop2/conf/hbase-site.xml");
		conf.set("hbase.master","localhost:60000");
		
		TableName tablename = TableName.valueOf("SuperCars");
		HTableDescriptor htd = new HTableDescriptor(tablename);
		
		htd.addFamily(new HColumnDescriptor("CarStats"));
		
		System.out.println("Connecting");
		
		try {
			HBaseAdmin hba = new HBaseAdmin(conf);
			
			boolean running = hba.isMasterRunning();
			if(running == true){
				System.out.println("The master is running");
			}else{
				System.out.println("The master is not running");
			}
			
			System.out.println("Creating table");
			hba.createTable(htd);
			System.out.println("Done");
			hba.close();
			System.out.println("Finished ... closing");
		
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
