package filters.comparsionfilters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseHelper;

/**
 * @author gaowenfeng
 */
public class DependentColumnFilterExample2 {
    private static final String TABLE_NAME = "testtable";
    private static final String COLUMN1 = "COLUMN1";
    private static final String COLUMN2 = "COLUMN2";
    private static final String COLUMN3 = "COLUMN3";

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();

        try(HBaseHelper helper = HBaseHelper.getHelper(configuration);
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME))){
            helper.dropTable(TABLE_NAME);
            helper.createTable(TABLE_NAME,COLUMN1,COLUMN2,COLUMN3);
            helper.put(TABLE_NAME,new String[]{"row1","row2","row3"},new String[]{COLUMN1},new String[]{"qua1"},new long[]{1},new String[]{"val1"});
            helper.put(TABLE_NAME,new String[]{"row1","row2","row3"},new String[]{COLUMN2,COLUMN3},new String[]{"qua2"},new long[]{1},new String[]{"val2"});

            Filter filter = new DependentColumnFilter(Bytes.toBytes(COLUMN1),Bytes.toBytes("qua1"),true);
            Scan scan = new Scan();
            scan.setFilter(filter);

            ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
                for(Cell cell:result.rawCells()){
                    System.out.println("cell: "+cell+",value:"
                            +Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
                }
            }
            scanner.close();
        }
    }
}
