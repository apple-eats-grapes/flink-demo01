package com.yjx.day02;
/*
package day02;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
public class Hello07TransformationAgg {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("math2", 200));
        list.add(new Tuple2<>("chinese2", 20));
        list.add(new Tuple2<>("math1", 100));
        list.add(new Tuple2<>("chinese1", 10));
        list.add(new Tuple2<>("math4", 400));
        list.add(new Tuple2<>("chinese4", 40));
        list.add(new Tuple2<>("math3", 300));
        list.add(new Tuple2<>("chinese3", 30));
        DataStreamSource<Tuple2<String, Integer>> aggregationSource = environment.fromCollection(list);
        KeyedStream<Tuple2<String, Integer>, Integer> keyedStream = aggregationSource.keyBy(new KeySelector<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0.length();
            }
        });
        // keyedStream.sum(1).print("sum-");
        // keyedStream.max(1).print("max-");
        keyedStream.maxBy(1).print("maxBy-");
        // keyedStream.min(1).print("min-");
        // keyedStream.minBy(1).print("minBy-");

        //运行环境
        environment.execute();
    }
}*/
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.ZoneId;

/*
*aggregation()算子  用于聚合计算，相比于reduce的聚合计算，其能实现更加复杂的计算。
*
* */
public class Hello07TransformationAgg {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据流，这里使用了Tuple2来表示一个带有键和值的数据流
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 19523)
                .map(line -> {
                    String[] parts = line.split(",");
                    return new Tuple2<>(parts[0], Integer.parseInt(parts[1]));
                },Types.TUPLE(Types.STRING,Types.INT));

        // 按照键进行滚动窗口操作，并使用aggregation算子进行求和
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy(tuple -> tuple.f0) // 根据键分组
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) // 滚动窗口大小为5秒
                .aggregate(new MyAggregator()); // 使用自定义的AggregateFunction进行聚合

        // 打印结果
        resultStream.print();

        // 执行任务
        env.execute("Aggregation Example");
    }

    private static class  MyAggregator implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }
        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            // 将输入数据的值累加到累加器中
            return accumulator + value.f1;
        }
        @Override //返回累加器中的值
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }
        @Override
        public Integer merge(Integer a, Integer b) {
            // 在窗口合并操作中，合并两个累加器的值，这里的合并应该是上一个窗口与下一个窗口的合并
            return a + b;
        }
    }




   /*

    //聚合函数，使用

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置并行度为 1（仅用于演示）

        // 准备输入数据
        DataStream<Tuple2<String, Integer>> inputDataStream = env.fromElements(
                Tuple2.of("Alice", 85),
                Tuple2.of("Bob", 92),
                Tuple2.of("Charlie", 78),
                Tuple2.of("David", 90)
        ).returns(Types.TUPLE(Types.STRING, Types.INT)); // 显式指定类型

        // 执行滚动聚合操作
        DataStream<Tuple4<String, Integer, String, Integer>> resultStream = inputDataStream
                .keyBy(tuple -> true)
                .process(new AggregationFunction());//创建

        // 打印结果
        resultStream.print();
        // 执行任务
        env.execute("Rolling Aggregation Example");
    }
    public static class AggregationFunction extends KeyedProcessFunction<Boolean, Tuple2<String, Integer>, Tuple4<String, Integer, String, Integer>> {
        private Integer maxScore;
        private String maxStudent;
        private Integer minScore;
        private String minStudent;

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple4<String, Integer, String, Integer>> out) throws Exception {
            // 获取当前状态
            if (maxScore == null || value.f1 > maxScore) {
                maxScore = value.f1;
                maxStudent = value.f0;
            }
            if (minScore == null || value.f1 < minScore) {
                minScore = value.f1;
                minStudent = value.f0;
            }

            // 输出结果
            out.collect(Tuple4.of(maxStudent, maxScore, minStudent, minScore));
        }*/

}