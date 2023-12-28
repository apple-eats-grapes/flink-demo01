package com.yjx.day03;


import com.yjx.util.KafkaUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Locale;

/*
* 定义水位线，数据是乱序，使用forBoundedOutOfOrderness()
* */
public class Hello14WaterMarkOutOrder {
    public static void main(String[] args) throws Exception {

        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);
            for (int i = 100; i < 200; i++) {
                if (i % 5 != 0) {
                    KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + System.currentTimeMillis());
                } else {
                    KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + (System.currentTimeMillis() - (long) (Math.random() * 10000)));
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

        //获取数据源
        DataStreamSource<String> source = environment.fromSource(KafkaUtil.getKafkaSource("yjxxt", "yjx")
                , WatermarkStrategy.noWatermarks()//不使用水位线，因为没有
                , "kafka source");
        source.map(line-> Tuple3.of(
                        line.split(":")[0]
                        ,line.split(":")[1]
                        ,Long.parseLong(line.split(":")[2])
                        )
                        ,Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                                <Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        //设定水位线延迟时间为5秒，根据事件时间，以及窗口的末端范围确定该窗口中的水位线数据，
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                                return element.f2;//提取数据的时间戳作为事件事件
                            }
                        }))
                .keyBy(tuple3->tuple3.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//设定10秒的窗口,根据数据中的事件时间
                .apply(new WindowFunction<Tuple3<String, String, Long>, String , String, TimeWindow>() {
                    //这里使用apply对每条数据进行特殊处理后输出，不使用reduce是应为其是用于对数据的聚合操作
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Long>> input, Collector<String> out) throws Exception {

                        StringBuffer buffer = new StringBuffer();

                        for (Tuple3<String,String,Long> tuple:input){
                            buffer.append("[key:"+tuple.f0+",name:"+tuple.f1+",time:"+tuple.f2+"]");
                        }
                        buffer.append("[window:"+window +"]");
                    }
                }).print();
        //运行环境
        environment.execute();

    }
}
