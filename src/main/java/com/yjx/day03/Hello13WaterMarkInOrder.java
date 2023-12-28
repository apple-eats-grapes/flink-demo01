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

import java.util.Locale;

/*
* 水位线设定
* 假定数据是有序的，窗口中的最大事件事件就是水位线，使用forMonotonousTimestamps（）
* 事件事件需要从数据中获取时间戳然后进行转换，才能用于后面的窗口使用事件时间统计，这里转换后就可以设定延迟时间，不设定，就是最大的时间戳
* */
public class Hello13WaterMarkInOrder {
    public static void main(String[] args) throws Exception {

        //生产Kafka有序数据数据--模拟弹幕[用户名:消息:时间戳]
        new Thread(() -> {
            String uname = RandomStringUtils.randomAlphabetic(8).toLowerCase(Locale.ROOT);//随机生成8位到小写组合字符串，并将其转换成小写。
            for (int i = 100; i < 201; i++) {
                KafkaUtil.sendMsg("yjxxt", uname + ":" + i + ":" + System.currentTimeMillis());
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

        //读取数据
        DataStreamSource<String> source = environment.
                fromSource(KafkaUtil.getKafkaSource("yjxxt", "yjx")
                        , WatermarkStrategy.noWatermarks()//这里由于数据中没有设置水位线，所以获取不到
                        ,"Kafka Source");
        //转换数据
        source.map(line->{
            return Tuple3.of(line.split(":")[0]
                    ,line.split(":")[1]
                    ,Long.parseLong(line.split(":")[2]));
        }, Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.
                        <Tuple3<String ,String ,Long>>forMonotonousTimestamps()//处理有序单调递增的数据，
                        // forMonotonousTimestamps()这里设定的是依据最大时间戳来生成水位线，所以这里没有设定延迟。
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                            //提取数据中的时间戳，并将其作为数据的事件时间，方便后续生成水位线
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }))
                .keyBy(tupes3->tupes3.f0)//更具key值进行分区
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//定义窗口大小，使用事件时间
                .apply(new WindowFunction<Tuple3<String, String, Long>, String , String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow window,
                                      Iterable<Tuple3<String, String, Long>> input,
                                      Collector<String> out) throws Exception {
                        StringBuffer buffer=new StringBuffer();
                        for (Tuple3<String ,String ,Long > tuple3:input){
                            buffer.append("[key:"+tuple3.f1+ ",num:"+tuple3.f1 + ",time:" + tuple3.f2+"]");
                        }
                        buffer.append("["+window+ "]");
                        //返回结果
                        out.collect(buffer.toString());
                    }
                }).print();

        environment.execute();



    }
}
