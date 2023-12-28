package com.yjx.day05_06.flink;

import com.yjx.udf.TableAggregateFunction4Order2Price;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello28UDFTableAggregateFunction {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_order (\n" +
                " id INT,\n" +
                " type INT,\n" +
                " price INT\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.id.kind'='sequence',\n" +
                " 'fields.id.start'='1',\n" +
                " 'fields.id.end'='1000',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='3',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='200'\n" +
                ")");

        //普通查询
        // tableEnvironment.sqlQuery("select * from t_order").execute().print();

        // 注册函数
        tableEnvironment.createTemporarySystemFunction("tafop", TableAggregateFunction4Order2Price.class);
        tableEnvironment.sqlQuery("select type,tafop(price) from t_order group by type").execute().print();
    }
}
