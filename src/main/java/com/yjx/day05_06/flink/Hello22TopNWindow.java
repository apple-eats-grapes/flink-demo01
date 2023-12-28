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
public class Hello22TopNWindow {
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
                " ts AS localtimestamp,\n" +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='10',\n" +
                " 'fields.gid.length'='10',\n" +
                " 'fields.type.min'='1',\n" +
                " 'fields.type.max'='5',\n" +
                " 'fields.price.min'='100',\n" +
                " 'fields.price.max'='999'\n" +
                ")");

        //窗口数据
        // tableEnvironment.sqlQuery("SELECT window_start,window_end,type,max(price)  FROM TABLE " +
        //         "(TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '10' SECONDS)) " +
        //         "GROUP BY window_start, window_end, type"
        // ).execute().print();

        //查询10秒内 销售额最高的前三种种类
        // tableEnvironment.sqlQuery("select * from (" +
        //         "   select *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY sp DESC) as rownum from (" +
        //         "       SELECT window_start,window_end,type,sum(price) as sp  FROM TABLE " +
        //         "       (TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '10' SECONDS)) " +
        //         "       GROUP BY window_start, window_end, type" +
        //         "   )" +
        //         ") where rownum <=3 ").execute().print();

        //查询10秒内 每个种类销售价格最高的前三名
        tableEnvironment.sqlQuery("select window_start,window_end,type,gid,price,rownum from (" +
                "   SELECT * , ROW_NUMBER() OVER (PARTITION BY window_start, window_end,type ORDER BY price DESC) as rownum FROM TABLE " +
                "   (TUMBLE(TABLE t_goods, DESCRIPTOR(ts), INTERVAL '10' SECONDS)) " +
                ") where rownum <=3"
        ).execute().print();

    }
}
