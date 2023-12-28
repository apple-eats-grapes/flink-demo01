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
public class Hello18GroupDistinct {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_access (\n" +
                " uid INT,\n" +
                " url STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.uid.min'='1000',\n" +
                " 'fields.uid.max'='2000',\n" +
                " 'fields.url.length'='10'\n" +
                ")");
        // tableEnvironment.sqlQuery("select * from t_access").execute().print();

        //统计网站的UV和PV数
        tableEnvironment.sqlQuery("select count(url) as pv,count(distinct uid) from t_access").execute().print();

    }
}
