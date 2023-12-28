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
public class Hello15TVFTumble {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //执行SQL
        tableEnvironment.executeSql("CREATE TABLE t_goods (\n" +
                " gid INT,\n" +
                " sales INT,\n" +
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.gid.min'='10',\n" +
                " 'fields.gid.max'='20',\n" +
                " 'fields.sales.min'='1',\n" +
                " 'fields.sales.max'='9'\n" +
                ")");
        // tableEnvironment.sqlQuery("select * from t_goods").execute().print();

        //按照窗口进行查询--所有信息
        // tableEnvironment.sqlQuery("SELECT * FROM TABLE(TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS));").execute().print();

        //按照窗口进行查询--分组查询
        // tableEnvironment.sqlQuery("SELECT window_start,window_end,sum(sales)  FROM TABLE " +
        //         "(TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS)) " +
        //         "GROUP BY window_start, window_end;"
        // ).execute().print();

        //按照窗口进行查询--分组查询
        tableEnvironment.sqlQuery("SELECT window_start,window_end,gid,sum(sales)  FROM TABLE " +
                "(TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '5' SECONDS)) " +
                "GROUP BY window_start, window_end,gid;"
        ).execute().print();

    }
}
