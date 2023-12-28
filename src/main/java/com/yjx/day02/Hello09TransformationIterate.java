package com.yjx.day02;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Hello09TransformationIterate {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //操作数据
        DataStreamSource<String> source = environment.fromElements("香蕉,51", "苹果,101", "桃子,202");
        DataStream<Tuple3<String, Integer, Integer>> mapStream = source.map(word -> {
            String[] split = word.split(",");
            return Tuple3.of(split[0], Integer.parseInt(split[1]), 0);
        }, Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        //开始将数据进行迭代
        IterativeStream<Tuple3<String, Integer, Integer>> iterativeStream = mapStream.iterate();
        //定义循环体--假设水果每天销售10斤
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterativeBody = iterativeStream.map(tuple3  -> {
            tuple3.f1 -= 10;
            tuple3.f2++;
            return tuple3;
        }, Types.TUPLE(Types.STRING, Types.INT, Types.INT));
        //定义判断条件--剩余斤数>10可以继续迭代
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterativeFilter = iterativeBody.filter(tuple3 -> tuple3.f1 > 10);
        //开始迭代
        iterativeStream.closeWith(iterativeFilter);

        //找出不满足条件的数据
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> output = iterativeBody.filter(tuple3 -> tuple3.f1 <= 10);
        output.print("不满足条件的数据:");
        //运行环境
        environment.execute();

    }
}