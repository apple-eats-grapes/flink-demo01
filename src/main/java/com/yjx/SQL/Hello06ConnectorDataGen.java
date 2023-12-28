package com.yjx.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
*
* 使用dataGen连接器，自动创建数据
*
* */
public class Hello06ConnectorDataGen {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行sql

        tableEnvironment.executeSql("CREATE TABLE datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +//指定要使用的来凝结器
                " -- optional options --\n" +
                " 'rows-per-second'='5',\n" +//每秒生成的行数，用以控制数据发出速率
                " 'fields.f_sequence.kind'='sequence',\n" +// 自定字段生成器的生成类型，默认random（乱序），sequence（升序），如果是乱序，则不用写该列
                " 'fields.f_sequence.start'='1',\n" +//使用乱序的话生成数据，就不用start和end，而是直接max和min来限定数据范围
                " 'fields.f_sequence.end'='1000',\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                " 'fields.f_random_str.length'='10'\n" +//随机生成字符长度
                ")");
        tableEnvironment.executeSql("select * from datagen").print();

    }

}
