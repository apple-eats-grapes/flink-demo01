package com.yjx.day04.flink;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello03StateKeyed {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.enableCheckpointing();
        environment.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");
        //获取数据源【水果:重量】
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //计算
        source.map(line -> {
                    String[] split = line.split(":");
                    return Tuple2.of(split[0], Integer.parseInt(split[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .reduce(new YjxxtKeyedStateFunction())
                .print();
        //运行环境
        environment.execute();
    }
}

/**
 * 有钱可以为所欲为
 */
class YjxxtKeyedStateFunction extends RichReduceFunction<Tuple2<String, Integer>> {

    //声明一个状态对象
    private ValueState<Tuple2<String, Integer>> valueState;

    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        //开始计算
        value1.f1 = value1.f1 + value2.f1;

        //保存状态[自己动手丰衣足食]
        valueState.update(value1);

        return value1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化状态对象
        ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("reduceValueState", Types.TUPLE(Types.STRING, Types.INT));
        this.valueState = getRuntimeContext().getState(descriptor);
    }
}