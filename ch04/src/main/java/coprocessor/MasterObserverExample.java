package coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

// cc MasterObserverExample Example master observer that creates a separate directory on the file system when a table is created.

/**
 * 创建新表时创建一个单独的目录
 * @author gaowenfeng
 */
public class MasterObserverExample extends BaseMasterObserver {
  public static final Log LOG = LogFactory.getLog(HRegion.class);

  @Override
  public void postCreateTable(
    ObserverContext<MasterCoprocessorEnvironment> ctx,
    HTableDescriptor desc, HRegionInfo[] regions)
    throws IOException {
    // ^^ MasterObserverExample
    LOG.debug("Got postCreateTable callback");
    // vv MasterObserverExample
    // co MasterObserverExample-1-GetName Get the new table's name from the table descriptor.
    // 从表描述符中获取表名
    TableName tableName = desc.getTableName();

    LOG.debug("Created table: " + tableName + ", region count: " + regions.length);

    MasterServices services = ctx.getEnvironment().getMasterServices();
    // co MasterObserverExample-2-Services Get the available services and retrieve a reference to the actual file system.
    // 获取可用的服务，同事取得真是文件系统的引用
    MasterFileSystem masterFileSystem = services.getMasterFileSystem();
    FileSystem fileSystem = masterFileSystem.getFileSystem();

    // co MasterObserverExample-3-Path Create a new directory that will store binary data from the client application.
    // 创建新目录用来存储客户端应用的二进制数据
    Path blobPath = new Path(tableName.getQualifierAsString() + "-blobs");
    fileSystem.mkdirs(blobPath);

    // ^^ MasterObserverExample
    LOG.debug("Created " + blobPath + ": " + fileSystem.exists(blobPath));
    // vv MasterObserverExample
  }
}
// ^^ MasterObserverExample
