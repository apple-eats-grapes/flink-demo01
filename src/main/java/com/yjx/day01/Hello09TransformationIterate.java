package com.yjx.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//循环条件

/*
* 仓库中每种水果数量一定，每天指定售卖10斤，筛选出仓库中刚好只能能够售10天的水果种类。
* */
public class Hello09TransformationIterate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStreamSource<String> source = environment.fromElements("香蕉,51", "苹果,101", "桃子,202", "猕猴桃,103");
        SingleOutputStreamOperator<Tuple3<String,Integer, Integer>> map = source.map(fruit -> {
            String[] split = fruit.split(",");
            return Tuple3.of(split[0], Integer.parseInt(split[1]), 0);
        }, Types.TUPLE(Types.STRING, Types.INT, Types.INT));


        //创建迭代流
        IterativeStream<Tuple3<String, Integer, Integer>> iterateStream = map.iterate();
        //循环体
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> iterateBody = iterateStream.map(tupes3 -> {
            tupes3.f1 -= 10;
            tupes3.f2++;
            return tupes3;
        }, Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        //定义循环条件条件
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> filter = iterateBody.filter(tuple3 -> tuple3.f1 > 10);

        //开始迭代循环
        iterateStream.closeWith(filter);

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> output = iterateBody.filter(typle3 -> typle3.f2 >= 10 && typle3.f1 < 10);
        output.print("刚好能卖10天的水果是");

        environment.execute();
    }
}
