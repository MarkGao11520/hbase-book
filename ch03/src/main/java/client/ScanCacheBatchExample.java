package client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import util.HBaseHelper;

/**
 * cc ScanCacheBatchExample Example using caching and batch parameters for scans
 * @author gaowenfeng
 */
public class ScanCacheBatchExample {

  private static Table table = null;


  /**
   * ScanCacheBatchExample
   * @param caching
   * @param batch`
   * @param small
   * @throws IOException
   */
  private static void scan(int caching, int batch, boolean small)
  throws IOException {
    int count = 0;
    // co ScanCacheBatchExample-1-Set Set caching and batch parameters.
    Scan scan = new Scan()
      .setCaching(caching)
      .setBatch(batch)
      .setSmall(small)
      .setScanMetricsEnabled(true);
    ResultScanner scanner = table.getScanner(scan);
    for (Result result : scanner) {
      while (result.advance()){
        Cell cell = result.current();
        System.out.print("\tCell:"+cell);
      }
      System.out.println("\n");
      // co ScanCacheBatchExample-2-Count Count the number of Results available.
      count++;
    }
    scanner.close();
    ScanMetrics metrics = scan.getScanMetrics();
    /**
     * all_cols（一个表中所有单元格数 ，表被填满情况下） = rows(行数)*cols (每行列数)
     * Results = all_cols/min{batch,cols} （batch!=0）；Results = rows (batch==0)
     * RPCs = (Results/Caching)+3(一次打开，一次关闭，一次确认扫描完成)（small==false）；RPCs = 0(small==true)
     */
    System.out.println("Caching: " + caching + ", Batch: " + batch +
      ", Small: " + small + ", Results: " + count +
      ", RPCs: " + metrics.countOfRPCcalls);
  }

  public static void main(String[] args) throws IOException {
    // ^^ ScanCacheBatchExample
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    table = connection.getTable(TableName.valueOf("testtable"));

    // vv ScanCacheBatchExample
    /*...*/
    scan(1, 1, false);
    scan(1, 0, false);
    scan(1, 0, true);
    scan(200, 1, false);
    scan(200, 0, false);
    scan(200, 0, true);
    // co ScanCacheBatchExample-3-Test Test various combinations.
    scan(2000, 100, false);
    scan(2, 100, false);
    scan(2, 10, false);
    scan(5, 100, false);
    scan(5, 20, false);
    scan(10, 10, false);
    /*...*/
    // ^^ ScanCacheBatchExample
    table.close();
    connection.close();
    helper.close();
    // vv ScanCacheBatchExample
  }
  // ^^ ScanCacheBatchExample
}
