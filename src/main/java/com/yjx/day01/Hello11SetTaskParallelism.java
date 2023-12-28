package com.yjx.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/*
* 设置任务并行度
* */
public class Hello11SetTaskParallelism {
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);//设置全局并行度
        //配置score
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //transformation
        source.flatMap((line,collectot)->{
            Arrays.stream(line.split(" ")).forEach(collectot::collect);
        },Types.STRING).map(word-> Tuple2.of(word,1), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                        .sum(1).setParallelism(3)//设置map的并行度为3，修改原来的全局并行度2
                        .print().setParallelism(3);//设置输出并行度为3
        //sink

        //执行
//        environment.execute();
        System.out.println(environment.getExecutionPlan());//打印执行计划
    }

}
