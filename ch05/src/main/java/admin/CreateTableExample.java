package admin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import util.HBaseHelper;

// cc CreateTableExample Example using the administrative API to create a

/**
 * 使用客户端API建表
 * @author gaowenfeng
 */
public class CreateTableExample {

  public static void main(String[] args) throws IOException, InterruptedException {
    // vv CreateTableExample
    Configuration conf = HBaseConfiguration.create();
    // ^^ CreateTableExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    // vv CreateTableExample
    Connection connection = ConnectionFactory.createConnection(conf);
    // co CreateTableExample-1-CreateAdmin Create a administrative API instance.
    // 创建HBaseAdmin实例
    Admin admin = connection.getAdmin();

    TableName tableName = TableName.valueOf("testtable");
    // co CreateTableExample-2-CreateHTD Create the table descriptor instance.
    // 创建表描述符
    HTableDescriptor desc = new HTableDescriptor(tableName);

    // co CreateTableExample-3-CreateHCD Create a column family descriptor and add it to the table descriptor.
    // 添加列族描述符到表描述符中
    HColumnDescriptor coldef = new HColumnDescriptor(
      Bytes.toBytes("colfam1"));
    desc.addFamily(coldef);

    // co CreateTableExample-4-CreateTable Call the createTable() method to do the actual work.
    // 调用建表方法createTable()
    admin.createTable(desc);

    boolean avail = admin.isTableAvailable(tableName);
    // co CreateTableExample-5-Check Check if the table is available.
    // 检查表是否可用
    System.out.println("Table available: " + avail);
    // ^^ CreateTableExample
  }
}
