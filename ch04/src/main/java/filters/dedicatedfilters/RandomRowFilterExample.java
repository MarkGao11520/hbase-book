package filters.dedicatedfilters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * cc RandomRowFilterExample Example filtering rows randomly
 * @author gaowenfeng
 */
public class RandomRowFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 1000, 30, 0, true, "colfam1");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));
    // vv RandomRowFilterExample
    Filter filter = new RandomRowFilter(0.3f);

    for (int loop = 1; loop <= 3; loop++) {
      Scan scan = new Scan();
      scan.setFilter(filter);
      ResultScanner scanner = table.getScanner(scan);
      // ^^ RandomRowFilterExample
      System.out.print("Results of scan for loop: " + loop);
      // vv RandomRowFilterExample
      int count = 0;
      for (Result result : scanner) {
        //        System.out.println(Bytes.toString(result.getRow()));
        count++;
      }
      System.out.print(" count: "+count+"\n");
      scanner.close();
    }
    // ^^ RandomRowFilterExample
  }
}
