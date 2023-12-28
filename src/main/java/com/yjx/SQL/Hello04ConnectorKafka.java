package com.yjx.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello04ConnectorKafka {
    public static void main(String[] args) {

        //创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);

        //读取kafka中的数据，并创建表
        String kafaSqlSource = "CREATE TABLE kafa_dept (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_dept',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";

        //创建写入kafka中的数据表
        String kafaSqlSink = "CREATE TABLE kafa_dept_sink (\n" +
                "  `deptno` INT,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_dept_sink',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")";
            //执行创建表的sql语句
            streamTableEnvironment.executeSql(kafaSqlSource);
            streamTableEnvironment.executeSql(kafaSqlSink);
//            streamTableEnvironment.executeSql("INSERT INTO kafa_dept_sink SELECT deptno, loc FROM kafa_dept");//将查询结果插入表中，查询的结果必须与表的创建结构相同。
            streamTableEnvironment.executeSql("select * from kafa_dept").print();//从这里开始直接不打印后续数据，每当插入新数据这里就直接更新
//            System.out.println("===============================");
//            streamTableEnvironment.executeSql("select * from kafa_dept_sink").print();


    }
}
