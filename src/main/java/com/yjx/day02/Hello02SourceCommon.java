package com.yjx.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class Hello02SourceCommon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源--基于File
//        DataStreamSource<String> source = environment.readTextFile("data/wordcount.txt");

        //获取数据源--基于Socket
//        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);

        //获取数据源--基于Collection（集合）--基于此时或者代码的开发
        List<String> list = List.of("a", "b", "c");//这里获取的list结合数据不可修改
        DataStreamSource<String> source = environment.fromCollection(list);

        //转换和Sink
        source.map(word->word.toUpperCase()).print();

        //运行环境
        environment.execute();
    }
}
