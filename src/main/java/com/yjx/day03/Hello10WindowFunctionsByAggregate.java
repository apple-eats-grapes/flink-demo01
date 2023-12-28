package com.yjx.day03;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* 自定义窗口聚合函数  一般与滚动窗沟相聚合
* aggregateFunction  用于窗口计算，对窗口中的数据进行聚合，针对每个窗口。
* */
public class Hello10WindowFunctionsByAggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
            .keyBy(tuple2 -> tuple2.f0)
            .countWindow(3)
            .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer,Integer>, Double>() {
                /*
                * 这里是创建一个求平均值的函数
                * */
                @Override
                public Tuple2<Integer, Integer> createAccumulator() {
                    //初始化累加器
                    return Tuple2.of(0,0);
                }

                @Override
                public Tuple2<Integer, Integer> add(Tuple2<String, Integer> in, Tuple2<Integer, Integer> acc) {
                    //计算逻辑 第二个参数是输出值，输入值是上方转换好的元组
                    acc.f0=acc.f0+in.f1;
                    acc.f1+=1;
                    return acc;
                }

                @Override
                public Double getResult(Tuple2<Integer, Integer> acc) {
                    //获取累加器中的结果并计算平均值
                    if (acc.f1==0){
                        return 0.0;
                    }

                    return acc.f0 *1.0/ acc.f1;
                }

                @Override
                public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                    return null;
                }
            }).print("CountWindow--Tumbling").setParallelism(1);


        environment.execute();
    }
}
