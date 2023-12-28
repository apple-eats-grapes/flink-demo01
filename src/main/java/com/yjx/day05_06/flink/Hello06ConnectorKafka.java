package com.yjx.day05_06.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello06ConnectorKafka {
    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建表：Source
        tableEnvironment.executeSql("CREATE TABLE flink_source_table (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'kafka_source_topic',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");

        //创建表：SINK
        tableEnvironment.executeSql("CREATE TABLE flink_sink_table (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'kafka_sink_topic',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'yjxxtliyi',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        //运行代码--查询数据
        // tableEnvironment.sqlQuery("select * from flink_source_table").execute().print();

        //将一个表查询的数据输出到另外一张表
        tableEnvironment.sqlQuery("select * from flink_source_table").insertInto("flink_sink_table").execute();
    }
}
