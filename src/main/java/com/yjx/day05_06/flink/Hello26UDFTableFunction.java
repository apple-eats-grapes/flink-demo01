package com.yjx.day05_06.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.yjx.udf.TableFunction4Movie2Types;
/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello26UDFTableFunction {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //读取文件
        tableEnvironment.executeSql("CREATE TABLE t_movie (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  types STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'file:///C:\\Users\\SkyWorth\\IdeaProjects\\flink060106_sql\\data\\movie.txt',\n" +
                "  'format' = 'csv'\n" +
                ")");
        //简单查询
        // tableEnvironment.sqlQuery("select * from t_movie").execute().print();

        // 在 Table API 里不经注册直接“内联”调用函数
        // tableEnvironment
        //         .from("t_movie")
        //         .joinLateral(call(TableFunction4Movie2Types.class, $("types")).as("type", "score"))
        //         .select($("id"),$("name"),$("type"),$("score"))
        //         .execute()
        //         .print();

        // 注册函数
        tableEnvironment.createTemporarySystemFunction("tfmt", TableFunction4Movie2Types.class);
        tableEnvironment.sqlQuery("SELECT id,name,type,score FROM t_movie, LATERAL TABLE(tfmt(types))")
                .execute()
                .print();

    }
}
