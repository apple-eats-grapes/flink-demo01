package com.yjx.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
* 窗口函数
*
* */
public class Hello01Time {
    public static void main(String[] args) {
        //获取运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //时间语义
        DataStreamSource<String> source = environment.fromElements("aa", "bb", "cc");
        source.keyBy(w->w).window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


    }
}
