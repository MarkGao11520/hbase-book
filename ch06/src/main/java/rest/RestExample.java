package rest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc RestExample Example of using the REST client classes

/**
 * REST 客户端使用实例
 * @author gaowenfeng
 */
public class RestExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 100, 10, "colfam1");

    // vv RestExample
    Cluster cluster = new Cluster();
    // co RestExample-1-Cluster Set up a cluster list adding all known REST server hosts.
    // 设置已知的REST服务器集群地址列表
    cluster.add("localhost", 8080);

    // co RestExample-2-Client Create the client handling the HTTP communication.
    // 创建处理HTTP交互的客户端
    Client client = new Client(cluster);

    // co RestExample-3-Table Create a remote table instance, wrapping the REST access into a familiar interface.
    // 创建RemoteHTable实例，将REST访问封装到一个熟悉的接口中
    RemoteHTable table = new RemoteHTable(client, "testtable");

    // co RestExample-4-Get Perform a get operation as if it were a direct HBase connection.
    // 执行一个get()操作，如同直接连接HBase的操作
    Get get = new Get(Bytes.toBytes("row-30"));
    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-3"));
    Result result1 = table.get(get);

    System.out.println("Get result1: " + result1);

    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes("row-10"));
    scan.setStopRow(Bytes.toBytes("row-15"));
    scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("col-5"));
    // co RestExample-5-Scan Scan the table, again, the same approach as if using the native Java API.
    ResultScanner scanner = table.getScanner(scan);

    for (Result result2 : scanner) {
      System.out.println("Scan row[" + Bytes.toString(result2.getRow()) +
        "]: " + result2);
    }
    // ^^ RestExample
  }
}
