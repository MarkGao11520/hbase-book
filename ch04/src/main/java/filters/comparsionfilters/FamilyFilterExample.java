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
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * cc FamilyFilterExample Example using a filter to include only specific column families
 * @author gaowenfeng
 */
public class FamilyFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    try (HBaseHelper helper = HBaseHelper.getHelper(conf);
         Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(TableName.valueOf("testtable"))){
      helper.dropTable("testtable");
      helper.createTable("testtable", "colfam1", "colfam2", "colfam3", "colfam4");
      System.out.println("Adding rows to table...");
      helper.fillTable("testtable", 1, 10, 2, "colfam1", "colfam2", "colfam3", "colfam4");

      // vv FamilyFilterExample
      // co FamilyFilterExample-1-Filter Create filter, while specifying the comparison operator and comparator.
      // 创建一个列族比较器，指定比较器运算符和比较器（获取字典排序小于列族名为colfam3的）
      Filter filter1 = new FamilyFilter(CompareFilter.CompareOp.LESS,
        new BinaryComparator(Bytes.toBytes("colfam3")));

      Scan scan = new Scan();
      scan.setFilter(filter1);
      // co FamilyFilterExample-2-Scan Scan over table while applying the filter.
      // 使用过滤器扫描表
      ResultScanner scanner = table.getScanner(scan);
      // ^^ FamilyFilterExample
      System.out.println("Scanning table... ");
      // vv FamilyFilterExample
      for (Result result : scanner) {
        System.out.println(result);
      }
      scanner.close();

      Get get1 = new Get(Bytes.toBytes("row-5"));
      get1.setFilter(filter1);
      // co FamilyFilterExample-3-Get Get a row while applying the same filter.
      // 使用相同的过滤器获取一行数据
      Result result1 = table.get(get1);
      System.out.println("Result of get(): " + result1);

      Filter filter2 = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
        new BinaryComparator(Bytes.toBytes("colfam3")));
      // co FamilyFilterExample-4-Mismatch Create a filter on one column family while trying to retrieve another.
      // 在一个列族上创建过滤器，同时获取另一行数据
      Get get2 = new Get(Bytes.toBytes("row-5"));
      get2.addFamily(Bytes.toBytes("colfam1"));
      get2.setFilter(filter2);
      // co FamilyFilterExample-5-Get2 Get the same row while applying the new filter, this will return "NONE".
      // 使用新的过滤器获取同一行数据，此时返回结果为 "NONE"
      Result result2 = table.get(get2);
      System.out.println("Result of get(): " + result2);
    } finally {

    }
    // ^^ FamilyFilterExample
  }
}
