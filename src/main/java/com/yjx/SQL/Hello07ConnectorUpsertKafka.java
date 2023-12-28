package com.yjx.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Hello07ConnectorUpsertKafka {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        tableEnvironment.executeSql("CREATE TABLE t_dept (\n" +
                " deptno INT,\n" +
                " salenum INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +//使用本地时间戳作为水位线
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +//每秒生成的数据条数
                " 'fields.deptno.min'='88',\n" +//这里使用min和max来生成数据，使用的是乱序，就不用写fields.deptno.kind'='random
                " 'fields.deptno.max'='99',\n" +
                " 'fields.salenum.min'='1',\n" +
                " 'fields.salenum.max'='9'\n" +
                ")");

        //查询部门的销售详情
//        tableEnvironment.executeSql("select deptno,sum(salenum) as sumsale from t_dept group by deptno").print();

        //向插入kafka中插入数据
        tableEnvironment.executeSql("CREATE TABLE flink_dept_sale_sum (\n" +
                "  deptno INT,\n" +
                "  sumsale INT,\n" +
                "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'topic_dept_sale_sum',\n" +//没有该主题会自动创建
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'key.format' = 'csv',\n" +
                "  'value.format' = 'json'\n" +
                ");");

        //插入kafka中数据
        tableEnvironment.executeSql("insert into flink_dept_sale_sum select deptno,sum(salenum) as sumsale from t_dept group by deptno");


    }
}
