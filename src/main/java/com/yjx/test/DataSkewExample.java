package com.yjx.test;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

public class DataSkewExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", SocketProp.port);

//        source.map(line -> {
//                    String[] parts = line.split(":");
//                    System.out.println(line);
//                    return new Tuple2<>(parts[0], Integer.parseInt(parts[1]));
//                }, Types.TUPLE(Types.STRING, Types.INT))
//                .setParallelism(2)
//                .keyBy(t -> t.f0)
//                .reduce((t1,t2) -> {
////                    t1.f1 = t1.f1 +"_"+ t2.f1;
//                    t1.f1 = t1.f1 + t2.f1;
//                    return t1;
//                })
//                .setParallelism(2)
//                .print("\t");

        source
                .map(line -> {
                    String[] parts = line.split(":");
                    System.out.println(line);
                    String key = parts[0];
                    // TODO map阶段对a进行打散
                    if (key.equals("a"))
                        key = (new Random().nextInt(26*5)) + "_" + key;
                    return new Tuple2<>(key, Integer.parseInt(parts[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> {
                    // TODO ERROR  keyBy阶段 对 a进行打散  ！！ 不允许的
//                        if (value.f0.equals("a"))
//                            return (new Random().nextInt(10) + value.f0); // 使用键进行分区
                    return value.f0;
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .map((MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>) value -> {
                    // TODO 去掉盐
                    if (value.f0.matches("_"))
                        value.f0 = value.f0.split("_")[1];
                    return new Tuple2<>(value.f0, value.f1);
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(f -> f.f0) // 全局聚合的分区键
                .reduce((value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value2.f1))   // 聚合
                .print();


        env.execute("Data Skew Example");
    }
}
