package com.yjx.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
* 使用时间滑动窗口函数并自定义计算逻辑
* */
public class Hello12WindowFunctionsByWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);

        source.map(word-> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
            .keyBy(tuple2 -> tuple2.f0)
            .window(SlidingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(5)))
            .apply(new WindowFunction<Tuple2<String, Integer>, Tuple3<String ,Integer,String >, String, TimeWindow>() {
                @Override
                public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple3<String, Integer, String>> out) throws Exception {
                    //计算中和
                    int sum =0;
                    for (Tuple2<String ,Integer> tuple2:input ){
                        sum+=tuple2.f1;
                    }
                    out.collect(Tuple3.of(s,sum,window.toString()));
                }
            }).print("TimeWindow--Sliding").setParallelism(1);


        environment.execute();
    }
}
