package filters;

import com.google.protobuf.InvalidProtocolBufferException;

import filters.generated.FilterProtos;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteStringer;

/**
 * cc CustomFilter Implements a filter that lets certain rows pass
 * 实现一个只让一些特定行通过的过滤器
 * Implements a custom filter for HBase. It takes a value and compares
 * it with every value in each KeyValue checked. Once there is a match
 * the entire row is passed, otherwise filtered out.
 * @author gaowenfeng
 */
public class CustomFilter extends FilterBase {

  private byte[] value = null;
  private boolean filterRow = true;

  public CustomFilter() {
    super();
  }

  public CustomFilter(byte[] value) {
    // co CustomFilter-1-SetValue Set the value to compare against.
    // 设置要比较的值
    this.value = value;
  }

  @Override
  public void reset() {
    // co CustomFilter-2-Reset Reset filter flag for each new row being tested.
    // 每当有新行时重置过滤器的标志位
    this.filterRow = true;
  }

  @Override
  public ReturnCode filterKeyValue(Cell cell) {
    if (CellUtil.matchingValue(cell, value)) {
      // co CustomFilter-3-Filter When there is a matching value, then let the row pass.
      // 当有值匹配设定值时，让这一行通过过滤
      filterRow = false;
    }
    // co CustomFilter-4-Include Always include, since the final decision is made later.
    // 总是先包含KeyValue实例，知道filterRow()决定是否过滤这一行
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRow() {
    // co CustomFilter-5-FilterRow Here the actual decision is taking place, based on the flag status.
    // 这是实际上决定数据是否被返回的一行的代码，其基于标志位判断
    return filterRow;
  }

  @Override
  public byte [] toByteArray() {
    FilterProtos.CustomFilter.Builder builder =
      FilterProtos.CustomFilter.newBuilder();
    // co CustomFilter-6-Write Writes the given value out so it can be sent to the servers.
    // 写出给定值，以便将其发送到服务器
    if (value != null) {builder.setValue(ByteStringer.wrap(value)); }
    return builder.build().toByteArray();
  }

  //@Override
  public static Filter parseFrom(final byte[] pbBytes)
  throws DeserializationException {
    FilterProtos.CustomFilter proto;
    try {
      // co CustomFilter-7-Read Used by the servers to establish the filter instance with the correct values.
      // 服务端使用这个方法来初始化过滤器实例，所以客户端的设定值可以被独到
      proto = FilterProtos.CustomFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new CustomFilter(proto.getValue().toByteArray());
  }
}
// ^^ CustomFilter
