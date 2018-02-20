package filters.comparsionfilters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * cc DependentColumnFilterExample Example using a filter to include only specific column families
 * 参考列过滤器
 * @author gaowenfeng
 */
public class DependentColumnFilterExample {

  private static Table table = null;

  /**
   * 使用不同参数创建过滤器
   * @param drop 决定参考列被返回还是被丢弃 true 丢弃 false 返回
   * @param operator 比较运算符
   * @param comparator 比较器
   * @throws IOException
   */
  private static void filter(boolean drop,
      CompareFilter.CompareOp operator,
      ByteArrayComparable comparator)
  throws IOException {
    Filter filter;
    if (comparator != null) {
      // 使用多个参数创建过滤器
      // co DependentColumnFilterExample-1-CreateFilter Create the filter with various options.
      filter = new DependentColumnFilter(Bytes.toBytes("colfam1"),
        Bytes.toBytes("col-5"), drop, operator, comparator);
    } else {
      filter = new DependentColumnFilter(Bytes.toBytes("colfam1"),
        Bytes.toBytes("col-5"), drop);
    }

    Scan scan = new Scan();
    scan.setFilter(filter);
    // scan.setBatch(4); // cause an error
    ResultScanner scanner = table.getScanner(scan);
    // ^^ DependentColumnFilterExample
    System.out.println("Results of scan:");
    // vv DependentColumnFilterExample
    for (Result result : scanner) {
      for (Cell cell : result.rawCells()) {
        System.out.println("Cell: " + cell + ", Value: " +
          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength()));
      }
    }
    scanner.close();

    Get get = new Get(Bytes.toBytes("row-5"));
    get.setFilter(filter);
    Result result = table.get(get);
    // ^^ DependentColumnFilterExample
    System.out.println("Result of get: ");
    // vv DependentColumnFilterExample
    for (Cell cell : result.rawCells()) {
      System.out.println("Cell: " + cell + ", Value: " +
        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
          cell.getValueLength()));
    }
    // ^^ DependentColumnFilterExample
    System.out.println("");
    // vv DependentColumnFilterExample
  }

  public static void main(String[] args) throws IOException {
    // ^^ DependentColumnFilterExample
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, true, "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    table = connection.getTable(TableName.valueOf("testtable"));
    // vv DependentColumnFilterExample
    filter(true, CompareFilter.CompareOp.NO_OP, null);
    // co DependentColumnFilterExample-2-Filter Call filter method with various options.
    // 使用多个参数调用过滤器方法
    filter(false, CompareFilter.CompareOp.NO_OP, null);
    filter(true, CompareFilter.CompareOp.EQUAL,
      new BinaryPrefixComparator(Bytes.toBytes("val-5")));
    filter(false, CompareFilter.CompareOp.EQUAL,
      new BinaryPrefixComparator(Bytes.toBytes("val-5")));
    filter(true, CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(".*\\.5"));
    filter(false, CompareFilter.CompareOp.EQUAL,
      new RegexStringComparator(".*\\.5"));
  }
  // ^^ DependentColumnFilterExample
}
