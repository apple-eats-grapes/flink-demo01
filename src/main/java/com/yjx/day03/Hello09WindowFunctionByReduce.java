package com.yjx.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Hello09WindowFunctionByReduce {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);

        source.map(word-> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(3)//对分组后的数据进行统计数量到达3开始计算，否则就等待该key值的数量达到3才计算
                .reduce((t1,t2)->{
                    t1.f0=t1.f0+"_"+t2.f0;
                    t1.f1=t1.f1+ t2.f1;
                    return t1;
                }).print("CountWidow--Tumbling").setParallelism(1);


        //运行环境
        environment.execute();
    }
}
