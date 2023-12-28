package com.yjx.day04;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/*
* 数据状态
* */
public class Hello01StateOperator {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(2);//设置全局并行度
        environment.enableCheckpointing(5000);//设置检查点每5秒存储一次
        environment.getCheckpointConfig().setCheckpointStorage("file:///"+System.getProperty("user.dir")+ File.separator+"ckpt");
        //指定checketpoint文件存储位置
        // ，System.getProperty("user.dir")获取当前文件目录，
        // File.separator 表示分隔符，“ckpt”表示最终存储的文件目录名

        //获取数据源
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);


    }
}
