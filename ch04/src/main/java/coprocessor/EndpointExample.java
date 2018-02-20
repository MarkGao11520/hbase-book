package coprocessor;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;

import coprocessor.generated.RowCounterProtos;

import util.HBaseHelper;

// cc EndpointExample Example using the custom row-count endpoint

/**
 * 使用自定义行计数 endpoint
 * @author gaowenfeng
 */
public class EndpointExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("testtable");
    Connection connection = ConnectionFactory.createConnection(conf);
    // ^^ EndpointExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    helper.put("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      new String[]{"colfam1", "colfam2"},
      new String[]{"qual1", "qual1"},
      new long[]{1, 2},
      new String[]{"val1", "val2"});
    System.out.println("Before endpoint call...");
    helper.dump("testtable",
      new String[]{"row1", "row2", "row3", "row4", "row5"},
      null, null);
    Admin admin = connection.getAdmin();
    try {
      admin.split(tableName, Bytes.toBytes("row3"));
    } catch (IOException e) {
      e.printStackTrace();
    }
    // wait for the split to be done
    while (admin.getTableRegions(tableName).size() < 2) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    //vv EndpointExample
    Table table = connection.getTable(tableName);
    try {
      final RowCounterProtos.CountRequest request =
        RowCounterProtos.CountRequest.getDefaultInstance();
      // co EndpointExample-1-ClassName Define the protocol interface being invoked.
      // 定义将要被调用的协议接口
      // co EndpointExample-2-Rows Set start and end row key to "null" to count all rows.
      // 设置起始行健和终止行健为null，来统计所有的行
      // co EndpointExample-3-Batch Create an anonymous class to be sent to all region servers.
      // 创建一个发往所有region的匿名类
      Map<byte[], Long> results = table.coprocessorService(
        RowCounterProtos.RowCountService.class,
        null, null,
        new Batch.Call<RowCounterProtos.RowCountService, Long>() {
          // co EndpointExample-4-Call The call() method is executing the endpoint functions.
          // call方法将会执行endpoint 功能
          public Long call(RowCounterProtos.RowCountService counter)
          throws IOException {
            BlockingRpcCallback<RowCounterProtos.CountResponse> rpcCallback =
              new BlockingRpcCallback<RowCounterProtos.CountResponse>();
            counter.getRowCount(null, request, rpcCallback);
            RowCounterProtos.CountResponse response = rpcCallback.get();
            return response.hasCount() ? response.getCount() : 0;
          }
        }
      );

      long total = 0;
      // co EndpointExample-5-Print Iterate over the returned map, containing the result for each region separately.
      // 遍历返回的键值映射结果，其中包含了每个region的结果
      for (Map.Entry<byte[], Long> entry : results.entrySet()) {
        total += entry.getValue().longValue();
        System.out.println("Region: " + Bytes.toString(entry.getKey()) +
          ", Count: " + entry.getValue());
      }
      System.out.println("Total Count: " + total);
    } catch (Throwable throwable) {
      throwable.printStackTrace();
    }
  }
}
// ^^ EndpointExample
