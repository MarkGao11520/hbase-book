package client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * cc CheckAndDeleteExample Example application using the atomic compare-and-set operations
 * @author gaowenfeng
 */
public class CheckAndDeleteExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 100, "colfam1", "colfam2");
    helper.put("testtable",
      new String[] { "row1" },
      new String[] { "colfam1", "colfam2" },
      new String[] { "qual1", "qual2", "qual3" },
      new long[]   { 1, 2, 3 },
      new String[] { "val1", "val2", "val3" });
    System.out.println("Before delete call...");
    helper.dump("testtable", new String[]{ "row1" }, null, null);

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv CheckAndDeleteExample
    Delete delete1 = new Delete(Bytes.toBytes("row1"));
    // co CheckAndDeleteExample-1-Delete1 Create a new Delete instance.
    delete1.addColumns(Bytes.toBytes("colfam1"), Bytes.toBytes("qual3"));

    // co CheckAndDeleteExample-2-CAS1 Check if column does not exist and perform optional delete operation.
    // 不存在row1:colfam2:qual3 则删除 row1:colfam1:qual3
    boolean res1 = table.checkAndDelete(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), null, delete1);
    // co CheckAndDeleteExample-3-SOUT1 Print out the result, should be "Delete successful: false".
    System.out.println("Delete 1 successful: " + res1);

    Delete delete2 = new Delete(Bytes.toBytes("row1"));
    // co CheckAndDeleteExample-4-Delete2 Delete checked column manually.
    delete2.addColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"));
    table.delete(delete2);

    // co CheckAndDeleteExample-5-CAS2 Attempt to delete same cell again.
    boolean res2 = table.checkAndDelete(Bytes.toBytes("row1"),
      Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"), null, delete1);
    // co CheckAndDeleteExample-6-SOUT2 Print out the result, should be "Delete successful: true" since the checked column now is gone.
    System.out.println("Delete 2 successful: " + res2);

    Delete delete3 = new Delete(Bytes.toBytes("row2"));
    // co CheckAndDeleteExample-7-Delete3 Create yet another Delete instance, but using a different row.
    delete3.addFamily(Bytes.toBytes("colfam1"));

    try{
      // co CheckAndDeleteExample-8-CAS4 Try to delete while checking a different row.
      boolean res4 = table.checkAndDelete(Bytes.toBytes("row1"),
        Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
        Bytes.toBytes("val1"), delete3);
      // co CheckAndDeleteExample-9-SOUT4 We will not get here as an exception is thrown beforehand!
      System.out.println("Delete 3 successful: " + res4);
    } catch (Exception e) {
      // Action's getRow must match the passed row
      System.err.println("Error: " + e.getMessage());
    }
    // ^^ CheckAndDeleteExample
    table.close();
    connection.close();
    System.out.println("After delete call...");
    helper.dump("testtable", new String[]{"row1"}, null, null);
    helper.close();
  }
}
