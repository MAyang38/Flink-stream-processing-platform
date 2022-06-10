package bishe.function.sink;


import bishe.model.CarLocationInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSink extends RichSinkFunction<CarLocationInfo> {
    //初始化hbase连接conf和conn
    private static org.apache.hadoop.conf.Configuration conf;
    private static Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //连接配置，设置zk
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","worker2");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //进行连接
        conn = ConnectionFactory.createConnection(conf
//                new ThreadPoolExecutor(
//                        20,100,60,
//                        TimeUnit.MINUTES,
//                        new LinkedBlockingDeque<>(20)
//                )
        );
        System.out.println(conn.isClosed() +":::"+conn  );
    }

    @Override
    public void invoke(CarLocationInfo element, Context context) throws Exception {

//        Map<String, Object> jsonMap = maxwellEntity.getData();
        String tableName = "carinfo";
//        10000000000000
//        11640742197000
        String rowKey = "L_" + (10000000000000L-element.getTime()) + "_" +element.getTerminal();
        System.out.println(rowKey);
//        String columnFamily = "dim";  //默认维度表不区分
//        //开始进入处理
//        System.out.println(tableName+":::"+conn);
//        System.out.println(conn.isClosed());
        String location = element.getLatitude()+"_"+element.getLongitude();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
//        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("value"),Bytes.toBytes(jsonMap.toString()));

        put.addColumn("statistical".getBytes(),"count".getBytes(),Bytes.toBytes(18));
//        put.addColumn("statistical".getBytes(),"distance".getBytes(),Bytes.toBytes("地球村"));
        put.addColumn("statistical".getBytes(),"location".getBytes(),Bytes.toBytes(location));
//        put.addColumn("statistical".getBytes(),"location_type".getBytes(),Bytes.toBytes("10086"));
        //插入数据
//

        table.put(put);
        System.out.println("插入数据成功");
    }
    //关闭操作
    @Override
    public void close() throws Exception {
        if(conn != null){conn.close();}
    }
}