package com.yjx.day02;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Hello08TransformationReduce {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //操作数据
//        DataStreamSource<String> source = environment.fromElements("a", "aa", "b", "bb", "c", "cc");
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);

        //通过元素长度进行key分区
//        source.keyBy(word -> word.length()).reduce((v1, v2) -> v1 + "," + v2).print();
        source.flatMap((line,collect)->{
            Arrays.stream(line.split(" ")).forEach(collect::collect);
        },Types.STRING)
                .map(word -> Tuple2.of(word,1), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0).reduce((t1, t2) -> {
                    t1.f1= t1.f1+ t2.f1 ;
                    return t1;
                }).print();//使用reduce的聚合

        //运行环境
        environment.execute();
    }
}
