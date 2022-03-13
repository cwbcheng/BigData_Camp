package com.github.cwbcheng.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Program {
    static String cfInfo = "info";
    static String cfScore = "score";
    static String rowKey = "name";
    static TableName tableName = TableName.valueOf("chengwenbo:student");

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor(cfInfo);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(cfScore);
            hTableDescriptor.addFamily(hColumnDescriptor1);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        addRow(conn, "Tom", "20210000000001", "1", "75", "82");
        addRow(conn, "Jerry", "20210000000002", "1", "85", "67");
        addRow(conn, "Jack", "20210000000003", "2", "80", "80");
        addRow(conn, "Rose", "20210000000004", "2", "60", "61");
        addRow(conn, "程文博", "G20190379010002", "1", "66", "66");
        System.out.println("Data insert success");

        getName(conn, "Tom");
        getName(conn, "Jerry");
        getName(conn, "Jack");
        getName(conn, "Rose");
        getName(conn, "程文博");

        deleteName(conn, "Tom");
        deleteName(conn, "Jerry");
        deleteName(conn, "Jack");
        deleteName(conn, "Rose");
        deleteName(conn, "程文博");
        System.out.println("Delete Success");

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }

    private static void deleteName(Connection conn, String name) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(name));
        conn.getTable(tableName).delete(delete);
    }

    private static void getName(Connection conn, String name) throws IOException {
        Get get = new Get(Bytes.toBytes(name));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }
    }

    private static void addRow(Connection conn, String name, String id, String cl, String uScore, String pScore) throws IOException {
        Put put = new Put(Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes(cfInfo), Bytes.toBytes("student_id"), Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(cfInfo), Bytes.toBytes("class"), Bytes.toBytes(cl));
        put.addColumn(Bytes.toBytes(cfScore), Bytes.toBytes("understanding"), Bytes.toBytes(uScore));
        put.addColumn(Bytes.toBytes(cfScore), Bytes.toBytes("programming"), Bytes.toBytes(pScore));
        conn.getTable(tableName).put(put);
    }
}
