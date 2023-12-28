package com.yjx.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/*
* window函数的使用
*
* */
public class Hello04TimeWindow{
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //依据处理时间（该数据处理的时间），滚动时间窗口
        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))//拆分后的数据转成元组，并指定元组类型。
                .keyBy(tuple2 -> tuple2.f0)//通过元组的第一位进行分组聚合
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))//每5秒按照分区统计一次
                .reduce((t1,t2)->Tuple2.of(t1.f0,t1.f1+t2.f1))//聚合相同分区的数据
                .map(tuple2 -> {tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;//对聚合后的数据获取聚合后的当前时间戳并进行时间类型转换。并加上对应的分区替换原来的分区键
                return tuple2;}
                        ,Types.TUPLE(Types.STRING,Types.INT))//指定返回类型
                .print();
        //滑动时间窗口，统计当前时间前指定窗口的数据，指定时间步长进行统计。
        source.map(word -> Tuple2.of(word.split(":")[0],Integer.parseInt(word.split(":")[1])), Types.TUPLE(Types.STRING,Types.INT))//拆分后的数据转成元组，并指定元组类型。
                .keyBy(tuple2 -> tuple2.f0)//通过元组的第一位进行分组聚合
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5),Time.seconds(2)))//每5秒按照分区统计一次
                .reduce((t1,t2)->Tuple2.of(t1.f0,t1.f1+t2.f1))//聚合相同分区的数据
                .map(tuple2 -> {tuple2.f0 = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy年MM月dd日HH时mm分ss秒SSS毫秒")) + tuple2.f0;//对聚合后的数据获取聚合后的当前时间戳并进行时间类型转换。并加上对应的分区替换原来的分区键
                            return tuple2;}
                        ,Types.TUPLE(Types.STRING,Types.INT))//指定返回类型
                .print();






        environment.execute();
    }
}
