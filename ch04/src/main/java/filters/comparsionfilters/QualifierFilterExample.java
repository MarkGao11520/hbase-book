package filters.comparsionfilters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * // cc QualifierFilterExample Example using a filter to include only specific column qualifiers
 * @author gaowenfeng
 */
public class QualifierFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    try (HBaseHelper helper = HBaseHelper.getHelper(conf);
         Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(TableName.valueOf("testtable"))){
      helper.dropTable("testtable");
      helper.createTable("testtable", "colfam1", "colfam2");
      System.out.println("Adding rows to table...");
      helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");


      // vv QualifierFilterExample
      Filter filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
        new BinaryComparator(Bytes.toBytes("col-2")));

      Scan scan = new Scan();
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      // ^^ QualifierFilterExample
      System.out.println("Scanning table... ");
      // vv QualifierFilterExample
      for (Result result : scanner) {
        System.out.println(result);
      }
      scanner.close();

      Get get = new Get(Bytes.toBytes("row-5"));
      get.setFilter(filter);
      Result result = table.get(get);
      System.out.println("Result of get(): " + result);
      // ^^ QualifierFilterExample
    } finally {

    }
  }
}
