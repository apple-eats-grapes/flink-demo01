package com.yjx.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Hello06TransformationCommon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.fromElements("Hello Hadoop", "Hello Hive", "Hello HBase Phoenix", "Hello   ClickHouse");


        source.flatMap((line,collector)->{
            Arrays.stream(line.split(":")).forEach(collector::collect);
        }, Types.STRING)
        .filter(word->word!= null&& word.length()>0 )
                .map(word-> Tuple2.of(word,(int)(Math.random()*10)),Types.TUPLE(Types.TUPLE(Types.STRING,Types.INT)))
                        .keyBy(tuple2 -> tuple2.f0)
                                .sum(1)
                                        .print();
        //运行环境
        environment.execute();
    }
}
