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
public class Hello24JoinInterval {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid STRING,\n" +
                " type INT,\n" +
                " price INT,\n" +
                " ts1 AS localtimestamp,\n" +
                " WATERMARK FOR ts1 AS ts1 - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.gid.length'='10',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='100',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='999'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE t_types (\n" +
                " type INT,\n" +
                " tname STRING,\n" +
                " ts2 AS localtimestamp,\n" +
                " WATERMARK FOR ts2 AS ts2 - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.type.kind'='sequence',\n" +
                " 'fields.type.start'='1',\n" +
                " 'fields.type.end'='1000',\n" +
                " 'fields.tname.length'='10'\n" +
                ")");

        //全外连接
        tableEnvironment.sqlQuery("select * from t_goods g , t_types t " +
                "where g.type = t.type " +
                "and g.ts1 BETWEEN t.ts2 - INTERVAL '5' SECOND AND t.ts2 + INTERVAL '5' SECOND ").execute().print();//key值相同的情况下数据流g的数据时间在数据流t的时间前后5秒范围内都可以匹配

    }
}
