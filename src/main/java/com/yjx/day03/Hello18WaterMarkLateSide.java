package com.yjx.day03;

import com.yjx.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Locale;
/*

*/
/*
* 处理过分迟到的数据
*
* *//*

public class Hello18WaterMarkLateSide {
    public static void main(String[] args) throws Exception {

        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 100; i < 200; i++) {
                if (i % 10 == 0) {
                    KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + (System.currentTimeMillis() - 15000L));
                } else if (i % 5 == 0) {
                    KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + (System.currentTimeMillis() - 3000L));
                } else {
                    KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + System.currentTimeMillis());
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        //读取数据源
        DataStreamSource<String> source = environment.fromSource(KafkaUtil.getKafkaSource("yjxxt", "liyidd"), WatermarkStrategy.noWatermarks(), "Kafka Source");
        //声明侧输出对象
        OutputTag<Tuple3<String, String, Long>> outputTag = new OutputTag<>("w10d1l5") {
        };

        //转换数据
        SingleOutputStreamOperator<String> streamOperator = source.map(line -> {
                    return Tuple3.of(line.split(":")[0], line.split(":")[1], Long.parseLong(line.split(":")[2]));
                }, Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))//允许迟到为1
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> tuple3, long ts) {
                                return tuple3.f2;
                            }
                        }))
                .keyBy(tuple3 -> tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//窗口10秒
                .allowedLateness(Time.seconds(5))//允许迟到5秒
                .sideOutputLateData(outputTag)//使用侧输出流处理迟到的数据
                .apply(new WindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {
                        StringBuffer buffer = new StringBuffer();
                        buffer.append("[" + key + "]");
                        for (Tuple3<String, String, Long> tuple3 : input) {
                            buffer.append("[" + tuple3.f1 + "_" + tuple3.f2 + "]");
                        }
                        buffer.append("[" + window + "]");
                        //返回结果
                        out.collect(buffer.toString());
                    }
                });//聚合后的主流

        //主流数据
        streamOperator.print("Main:");

        //侧输出数据
        streamOperator.getSideOutput(outputTag).print("Side:");

        //运行环境
        environment.execute();
    }
}
*/
