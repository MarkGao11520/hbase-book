package client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

import java.io.IOException;

/**
 * @author gaowenfeng
 */
public class LocakExample {
    private static final String ROW1 = "row1";
    private static final String COLFAM1 = "colfam1";
    private static final String QUAL1 = "qual1";
    private static final String VAL1 = "val1";
    private static final String VAL2 = "val2";
    private static final String VAL3 = "val3";
    private static final String TABLENAME = "testtable";
    private static final Configuration conf = new Configuration();

    /**
     * 使用一个异步的线程更新同一个行，但是不显示加锁
     */
    static class UnlocakPut implements Runnable {

        @Override
        public void run() {
            try(Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(TableName.valueOf(TABLENAME))){
                Put put = new Put(Bytes.toBytes(ROW1));
                put.addColumn(Bytes.toBytes(COLFAM1),Bytes.toBytes(QUAL1),Bytes.toBytes(VAL3));
                long time = System.currentTimeMillis();
                System.out.println("Thread trying to put same row now .....");
                table.put(put);
                System.out.println("Wait time :"+(System.currentTimeMillis()-time)+"ms");
            }catch (Exception e){
                System.err.println("Thread error: "+e);
            }
        }

        public static void main(String[] args) throws IOException {
            HBaseHelper helper = HBaseHelper.getHelper(conf);

            helper.dropTable(TABLENAME);
            helper.createTable(TABLENAME,COLFAM1);

            try(Connection connection = ConnectionFactory.createConnection(conf);
                Table table = connection.getTable(TableName.valueOf(TABLENAME))){
                System.out.println("Taking out lock ....");
            }catch (Exception e){
                System.err.println("未知异常:"+e);
            }
        }
    }
}
