package com.yjx.day05_06.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello13ProcessTime {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建表：SQL [10,ACCOUNTING,NEWYORK]
        tableEnvironment.executeSql("CREATE TABLE t_dept_sql (\n" +
                "  `deptno` INT,\n" +
                "  `dname` STRING,\n" +
                "  `loc` STRING,\n" +
                "  `pt` AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'kafka_topic_dept',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'sql',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");
        // tableEnvironment.sqlQuery("select * from t_dept_sql").execute().print();

        //创建表：1
        // DataStreamSource<String> deptSource = environment.readTextFile("data/dept.txt");
        // Table table1 = tableEnvironment.fromDataStream(deptSource, $("str"), $("pt").proctime());
        // tableEnvironment.sqlQuery("select * from " + table1.toString()).execute().print();

        //创建表：2
        DataStreamSource<String> deptSource = environment.readTextFile("data/dept.txt");
        Table table2 = tableEnvironment.fromDataStream(deptSource, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .build());
        tableEnvironment.sqlQuery("select * from " + table2.toString()).execute().print();

    }
}
