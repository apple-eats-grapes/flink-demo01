package com.yjx.day05_06.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello14EventTime {
    public static void main(String[] args) {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        //创建表：SQL
        tableEnvironment.executeSql("CREATE TABLE t_dept_sql (\n" +
                "  `deptno` INT,\n" +
                "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp'," +
                "  `dname` STRING,\n" +
                "  `loc` STRING,\n" +
                "   WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND " +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_dept',\n" +
                "  'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',\n" +
                "  'properties.group.id' = 'sql',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")");
//         tableEnvironment.sqlQuery("select * from t_dept_sql").execute().print();

        //创建表：TableAPI
        DataStreamSource<String> source1 = environment.fromElements("zhangsan," + (System.currentTimeMillis()));//造一条数据，并给定当前时间戳并向前
        SingleOutputStreamOperator<String> stream1 = source1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String s, long l) {
                        return Long.parseLong(s.split(",")[1]);
                    }
                }));//提取是护具中的时间并延迟5秒作为水位线
        Table table1 = tableEnvironment.fromDataStream(stream1, $("str"), $("et").rowtime());//获取流中的数据，以及将流中的数据时间戳转成正常时间转换成事件事件属性
        tableEnvironment.sqlQuery("select * from " + table1.toString()).execute().print();
        System.out.println(System.currentTimeMillis());

        //创建表：TableAPI
        DataStreamSource<String> source2 = environment.fromElements("lisi," + (System.currentTimeMillis() - 100));//造一条数据，并给定水位线时间时间戳
        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = source2.map(line -> {
            String[] split = line.split(",");
            return Tuple2.of(split[0], Long.parseLong(split[1]));
        }, Types.TUPLE(Types.STRING, Types.LONG));
        Table table2 = tableEnvironment.fromDataStream(stream2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.BIGINT())
                .columnByExpression("pt", "now()")//处理时间
                .columnByExpression("et", "TO_TIMESTAMP(FROM_UNIXTIME(f1/1000))")//转成时间戳
                // .columnByExpression("etms1", "TO_TIMESTAMP(FROM_UNIXTIME(f1,'yyyy-MM-dd HH:mm:ss.SSS')")//转成时间戳【不支持】
                // .columnByExpression("etms2", "TO_TIMESTAMP(f1)")//转成时间戳[不支持]
                .watermark("et", "et - INTERVAL '5' SECOND")
                .build());
        // tableEnvironment.sqlQuery("select * from " + table2.toString()).execute().print();

    }
}
