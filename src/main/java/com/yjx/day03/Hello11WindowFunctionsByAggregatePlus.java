package com.yjx.day03;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
* aggregate函数完善
*
*
* */
public class Hello11WindowFunctionsByAggregatePlus {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);

        source.map(word-> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .countWindow(3)
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple3<String ,Integer,Integer>, Tuple2<String ,Double>>() {
                    //这里的中间值使用三个元素的元组是用来接收对应的数据key值
                    @Override
                    public Tuple3<String, Integer, Integer> createAccumulator() {
                        return Tuple3.of(null,0,0);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> add(Tuple2<String, Integer> in, Tuple3<String, Integer, Integer> acc) {
                        acc.f0=in.f0;
                        acc.f1+=in.f1;
                        acc.f2++;
                        return acc;
                    }

                    @Override
                    public Tuple2<String ,Double> getResult(Tuple3<String, Integer, Integer> acc) {
                        if (acc.f2==0){
                            return Tuple2.of(acc.f0,0.0);
                        }
                        return Tuple2.of(acc.f0,acc.f1*1.0/acc.f2);
                    }

                    @Override
                    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> stringStringIntegerTuple3, Tuple3<String, Integer, Integer> acc1) {
                        return null;
                    }
                }).print("CountWindow--Tumbling :").setParallelism(1);
        environment.execute();

    }
}
