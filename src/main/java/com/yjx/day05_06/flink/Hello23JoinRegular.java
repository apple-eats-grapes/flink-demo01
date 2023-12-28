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
public class Hello23JoinRegular {
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
                " 'fields.type.max'='999',\n" +
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

        //两张表的关联查询--内连接 未匹配到的数据一直存放在内存中
        // tableEnvironment.sqlQuery("select * from t_goods g INNER JOIN t_types t ON g.type = t.type ").execute().print();

        //左外连接
        // tableEnvironment.sqlQuery("select * from t_goods g LEFT JOIN t_types t ON g.type = t.type ").execute().print();

        //右外连接
        // tableEnvironment.sqlQuery("select * from t_goods g RIGHT JOIN t_types t ON g.type = t.type ").execute().print();

        //全外连接
        tableEnvironment.sqlQuery("select * from t_goods g FULL OUTER JOIN t_types t ON g.type = t.type ").execute().print();

    }
}
