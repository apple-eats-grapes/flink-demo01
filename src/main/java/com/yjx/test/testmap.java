package com.yjx.test;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;

public class testmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.socketTextStream("localhost",19523)
                        .flatMap((word, collector) -> {
                            Arrays.stream(word.split(" ")).forEach(collector::collect);
                        }, Types.STRING).map(word -> Tuple2.of(word,1),Types.TUPLE(Types.STRING,Types.INT))
                        .keyBy(tuple2 -> tuple2.f0)
//                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                        //滚动窗口窗口间隔为5秒
                        .sum(1)
                        .print();
        environment.execute();
    }
}
