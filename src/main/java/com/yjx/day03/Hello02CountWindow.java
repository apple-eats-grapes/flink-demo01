package com.yjx.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Hello02CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);


        source.map(word-> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(4,2)
                .reduce(
                        (t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1)
                ).print();
        environment.execute();
    }
}
