package client;

// cc IncrementMultipleExample Example incrementing multiple counters in one row
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

/**
 * 增加一行中多个计数器的计数
 * @author gaowenfeng
 */
public class IncrementMultipleExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "daily", "weekly");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));
    // vv IncrementMultipleExample
    Increment increment1 = new Increment(Bytes.toBytes("20150101"));

    increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 1);
    // co IncrementMultipleExample-1-Incr1 Increment the counters with various values.
    increment1.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
    increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 10);
    increment1.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 10);
    // ^^ IncrementMultipleExample
    Map<byte[], NavigableMap<byte[], Long>> longs =
      increment1.getFamilyMapOfLongs();
    for (byte[] family : longs.keySet()) {
      System.out.println("Increment #1 - family: " + Bytes.toString(family));
      NavigableMap<byte[], Long> longcols = longs.get(family);
      for (byte[] column : longcols.keySet()) {
        System.out.print("  column: " + Bytes.toString(column));
        System.out.println(" - value: " + longcols.get(column));
      }
    }
    // vv IncrementMultipleExample

    // co IncrementMultipleExample-2-Incr2 Call the actual increment method with the above counter updates and receive the results.
    // 使用上述的计数器更新值调用实际的新增方法，并得到返回结果
    Result result1 = table.increment(increment1);

    for (Cell cell : result1.rawCells()) {
      // co IncrementMultipleExample-3-Dump1 Print the cell and returned counter value.
      // 打印cell 和 返回的计数器的值
      System.out.println("Cell: " + cell +
        " Value: " + Bytes.toLong(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength()));
    }

    Increment increment2 = new Increment(Bytes.toBytes("20150101"));


    // co IncrementMultipleExample-4-Incr3 Use positive, negative, and zero increment values to achieve the wanted counter changes.
    // 使用 正，负和零增加值来修改计数器值
    increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("clicks"), 5);
    increment2.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1);
    increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("clicks"), 0);
    increment2.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), -5);

    Result result2 = table.increment(increment2);

    for (Cell cell : result2.rawCells()) {
      System.out.println("Cell: " + cell +
        " Value: " + Bytes.toLong(cell.getValueArray(),
          cell.getValueOffset(), cell.getValueLength()));
    }
    // ^^ IncrementMultipleExample
    table.close();
    connection.close();
    helper.close();
  }
}
