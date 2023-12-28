package com.yjx.day05_06.flink;


import com.yjx.udf.ScalarFunction4DataGen2RandomStr;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello25UDFScalarFunctions {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_datagen (\n" +
                " f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.f_sequence.kind'='sequence',\n" +
                " 'fields.f_sequence.start'='1',\n" +
                " 'fields.f_sequence.end'='1000',\n" +
                " 'fields.f_random.min'='1',\n" +
                " 'fields.f_random.max'='1000',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")");

        // 在 Table API 里不经注册直接“内联”调用函数
        // tableEnvironment.from("t_datagen")
        //         .select(call(StringLengthScalarFunction.class, $("f_random_str")))
        //         .execute().print();

        // 注册函数
        tableEnvironment.createTemporarySystemFunction("slsf", ScalarFunction4DataGen2RandomStr.class);
        // 在 Table API 里调用注册好的函数
        // tableEnvironment.from("t_datagen").select(call("slsf", $("f_random_str"))).execute().print();

        // 在 SQL 里调用注册好的函数
        tableEnvironment.sqlQuery("SELECT slsf(f_random_str) FROM t_datagen").execute().print();
    }
}


