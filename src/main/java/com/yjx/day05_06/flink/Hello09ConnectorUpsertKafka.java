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
public class Hello09ConnectorUpsertKafka {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_dept (\n" +
                " deptno INT,\n" +
                " salenum INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.deptno.min'='88',\n" +
                " 'fields.deptno.max'='99',\n" +
                " 'fields.salenum.min'='1',\n" +
                " 'fields.salenum.max'='9'\n" +
                ")");
        //查询部门销售详情
        // tableEnvironment.sqlQuery("select deptno,sum(salenum) as sumsale from t_dept group by deptno").execute().print();

        //插入到一张Kafka的表中
        tableEnvironment.executeSql("CREATE TABLE flink_dept_sale_sum (\n" +
                "  deptno INT,\n" +
                "  sumsale INT,\n" +
                "  PRIMARY KEY (deptno) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'topic_dept_sale_sum',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'key.format' = 'csv',\n" +
                "  'value.format' = 'json'\n" +
                ");");
        //插入数据
        tableEnvironment.executeSql("insert into flink_dept_sale_sum select deptno,sum(salenum) as sumsale from t_dept group by deptno");

    }
}
