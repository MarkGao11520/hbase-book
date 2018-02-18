package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * cc GetMaxResultsRowOffsetExample2 Retrieves parts of a row with offset and limit #2
 * @author gaowenfeng
 */
public class GetMaxResultsRowOffsetExample2 {

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", 3, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv GetMaxResultsRowOffsetExample2
    // co GetMaxResultsRowOffsetExample2-1-Loop Insert three versions of each column.
    for (int version = 1; version <= 3; version++) {
      Put put = new Put(Bytes.toBytes("row1"));
      for (int n = 1; n <= 1000; n++) {
        String num = String.format("%04d", n);
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual" + num),
          Bytes.toBytes("val" + num));
      }
      System.out.println("Writing version: " + version);
      table.put(put);
      Thread.sleep(1000);
    }

    Get get0 = new Get(Bytes.toBytes("row1"));
    get0.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual0001"));
    // co GetMaxResultsRowOffsetExample2-2-Get0 Get a column with all versions as a test.
    get0.setMaxVersions();
    Result result0 = table.get(get0);
    CellScanner scanner0 = result0.cellScanner();
    while (scanner0.advance()) {
      System.out.println("Get 0 Cell: " + scanner0.current());
    }

    Get get1 = new Get(Bytes.toBytes("row1"));
    // co GetMaxResultsRowOffsetExample2-3-Get1 Get ten cells, single version per column.
    get1.setMaxResultsPerColumnFamily(10);
    Result result1 = table.get(get1);
    CellScanner scanner1 = result1.cellScanner();
    while (scanner1.advance()) {
      System.out.println("Get 1 Cell: " + scanner1.current());
    }

    Get get2 = new Get(Bytes.toBytes("row1"));
    get2.setMaxResultsPerColumnFamily(10);
    // co GetMaxResultsRowOffsetExample2-4-Get2 Do the same but now retrieve all versions of a column.
    get2.setMaxVersions(3);
    Result result2 = table.get(get2);
    CellScanner scanner2 = result2.cellScanner();
    while (scanner2.advance()) {
      System.out.println("Get 2 Cell: " + scanner2.current());
    }

    // ^^ GetMaxResultsRowOffsetExample2
    table.close();
    connection.close();
    helper.close();
  }
}
