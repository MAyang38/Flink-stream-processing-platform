package bishe.function.other;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbasetest {
    public static void main(String[] args) throws IOException {


//        42.192.10.136
//        Configuration configuration = HBaseConfiguration.create();
////        configuration.set("hbase.master", "cloud:60000");
//        configuration.set("hbase.zookeeper.quorum", "master");
////        configuration.set("hbase.zookeeper.quorum", "cloud");
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");

//        String createTableName = "mytable2";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "worker2:60000");
        configuration.set("hbase.zookeeper.quorum", "worker2");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        //configuration.set("hbase.master", "10.10.2.66:600000");
        System.out.println("start create table ......");
        Connection conn = ConnectionFactory.createConnection(configuration);
        System.out.println(" conn = ConnectionFactory.createConnection(configuration);");
//        try {
            Admin admin = conn.getAdmin();
//            System.out.println("  Admin admin = conn.getAdmin();");
//
//            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(createTableName));
//            tableDescriptor.addFamily(new HColumnDescriptor("column1"));
//            tableDescriptor.addFamily(new HColumnDescriptor("column2"));
//            tableDescriptor.addFamily(new HColumnDescriptor("column3"));
//            admin.createTable(tableDescriptor);
//            System.out.println(" admin.createTable(tableDescriptor);");
//
//            admin.close();
//        } catch (MasterNotRunningException e) {
//            e.printStackTrace();
//        } catch (ZooKeeperConnectionException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println("end create table ......");




//        Admin admin = conn.getAdmin();
//        System.out.println(conn.isClosed() +":::"+conn);
//
//
//
//        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("carinfo"));
//        //?????????????????? f1 f2
//        HColumnDescriptor f1 = new HColumnDescriptor("warn");
//        HColumnDescriptor f2 = new HColumnDescriptor("statistical");
//        //????????????????????????HTableDescriptor?????????
//        hTableDescriptor.addFamily(f1);
//        hTableDescriptor.addFamily(f2);
//        //?????????
//        admin.createTable(hTableDescriptor);
//
//        System.out.println("????????????");


        //????????????
        Table myuser = conn.getTable(TableName.valueOf("carinfo"));
//        //??????put??????????????????rowkey
        Put put = new Put("count_9223370390469988807_11642023240".getBytes());
        //????????????
//        put.addColumn("warn".getBytes(),"direction".getBytes(), Bytes.toBytes(2));//1?????????
//        put.addColumn("warn".getBytes(),"speed".getBytes(),Bytes.toBytes("??????"));//????????????????????????
        put.addColumn("statistical".getBytes(),"count".getBytes(),Bytes.toBytes(18));
//        put.addColumn("statistical".getBytes(),"distance".getBytes(),Bytes.toBytes("?????????"));
//        put.addColumn("statistical".getBytes(),"location".getBytes(),Bytes.toBytes("10086"));
//        put.addColumn("statistical".getBytes(),"location_type".getBytes(),Bytes.toBytes("10086"));
        //????????????
        myuser.put(put);


//
//
        admin.close();
        conn.close();

    }
}
