package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
// ^^ PutExample
import util.HBaseHelper;
// vv PutExample

import java.io.IOException;

/**
 * cc PutExample Example application inserting data into HBase
 * @author gaowenfeng
 */
public class PutExample {

  public static void main(String[] args) throws IOException {
    Connection connection = null;
    Table table = null;
    HBaseHelper helper = null;
    try {
      // co PutExample-1-CreateConf: Create the required configuration.
      Configuration conf = HBaseConfiguration.create();

      helper = HBaseHelper.getHelper(conf);
      helper.dropTable("testtable");
      helper.createTable("testtable", "colfam1");

      connection = ConnectionFactory.createConnection(conf);
      // co PutExample-2-NewTable: Instantiate a new client.
      table = connection.getTable(TableName.valueOf("testtable"));

      // co PutExample-3-NewPut: Create put with specific row.
      Put put = new Put(Bytes.toBytes("row1"));

      // co PutExample-4-AddCol1: Add a column, whose name is "colfam1:qual1", to the put.
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
        Bytes.toBytes("val1"));

      // co PutExample-4-AddCol2: Add another column, whose name is "colfam1:qual2", to the put.
      put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
        Bytes.toBytes("val2"));

      // co PutExample-5-DoPut: Store row with column into the HBase table.
      table.put(put);
      // co PutExample-6-DoPut: Close table and connection instances to free resources.
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      table.close();
      connection.close();
      helper.close();
    }
  }
}
