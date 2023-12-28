package com.yjx.day05_06.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello11Schema {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建表：SQL [10,ACCOUNTING,NEWYORK]
        tableEnvironment.executeSql("CREATE TABLE t_dept_sql (\n" +
                "  `deptno` INT,\n" +
                "  `deptno_new` AS deptno * 100,\n" +
                "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp'," +
                "  `dname` STRING,\n" +
                "  `loc` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'kafka_topic_dept',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'sql',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");

        //创建表：TableAPI [10,ACCOUNTING,NEWYORK]
        tableEnvironment.createTable("t_dept_tableapi", TableDescriptor.forConnector("kafka")
                .schema(Schema.newBuilder()
                        .column("deptno", DataTypes.INT())
                        .columnByExpression("deptno_new", "deptno * 100000")
                        .columnByMetadata("event_time", DataTypes.TIMESTAMP_LTZ(3), "timestamp", true)
                        .column("dname", DataTypes.STRING())
                        .column("loc", DataTypes.STRING())
                        .build())
                .option("connector", "kafka")
                .option("topic", "kafka_topic_dept")
                .option("properties.bootstrap.servers", "node01:9092,node02:9092,node03:9092")
                .option("properties.group.id", "tableapi")
                .option("scan.startup.mode", "earliest-offset")
                .format("csv")
                .build());

        //查询信息
        tableEnvironment.sqlQuery("select * from t_dept_tableapi").execute().print();
    }
}
