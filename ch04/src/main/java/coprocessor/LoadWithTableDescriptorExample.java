package coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import util.HBaseHelper;

// cc LoadWithTableDescriptorExample Load a coprocessor using the table descriptor

/**
 * 检查特定get请求的region observer
 * @author gaowenfeng
 */
public class LoadWithTableDescriptorExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    // ^^ LoadWithTableDescriptorExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv LoadWithTableDescriptorExample
    TableName tableName = TableName.valueOf("testtable");

    // co LoadWithTableDescriptorExample-1-Define Define a table descriptor.
    // 定义表描述符
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("colfam1"));
    // co LoadWithTableDescriptorExample-2-AddCP Add the coprocessor definition to the descriptor, while omitting the path to the JAR file.
    // 将协处理器定义添加到描述符，同时省略jar文件路径的定义
    htd.setValue("COPROCESSOR$1", "|" +
      RegionObserverExample.class.getCanonicalName() +
      "|" + Coprocessor.PRIORITY_USER);

    // co LoadWithTableDescriptorExample-3-Admin Acquire an administrative API to the cluster and add the table.
    // 创建集群的管理API并添加这个表
    Admin admin = connection.getAdmin();
    admin.createTable(htd);

    // co LoadWithTableDescriptorExample-4-Check Verify if the definition has been applied as expected.
    // 检查定义的协处理器是否被正确添加
    System.out.println(admin.getTableDescriptor(tableName));
    admin.close();
    connection.close();
  }
}
// ^^ LoadWithTableDescriptorExample
