package com.yjx.day03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/*
* windowAll的使用
*
* */
public class Hello03CountWindowAll {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        /*
        * countWindowAll全量窗口
        * */
        //滚动窗口
    /*    source.map(word-> Tuple2.of(
                        word.split(":")[0]
                        ,Integer.parseInt(word.split(":")[1])
                        ), Types.TUPLE(Types.STRING,Types.INT))
                .countWindowAll(3)
                        .reduce((t1,t2)->Tuple2.of(t1.f0,t2.f1+ t2.f1)
                              ).print();*/
        //滑动窗口
        source.map(
                word-> Tuple2.of(//这里拆分输入的数据使用：号拆分，拆分的字符串中的第一位，为元组的第一位，
                        word.split(":")[0]
                        ,Integer.parseInt(word.split(":")[1]))//拆分后的第二位转位int类型后作为元组的第二位
                        ,Types.TUPLE(Types.STRING,Types.INT)//指定返回值类型
                )
                .countWindowAll(3,2)//设定滑动窗口大小和步长，不设置步长就是滚动窗口。
                        .reduce(
                                (t1,t2)->Tuple2.of(t1.f0, t1.f1+t2.f1)//聚合算子，这里的t1和t2就是上面拆分的后创建的元组，返回新的聚合元组
                        ).print();
        //运行环境
        environment.execute();
    }
}
