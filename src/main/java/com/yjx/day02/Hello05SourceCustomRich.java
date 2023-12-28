package com.yjx.day02;


import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.File;
import java.util.List;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */

/*
* 设置任务并行度
* RichMapFunction的运用
*主要用于获取上下文环境
* */
public class Hello05SourceCustomRich {
    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<String> source = environment.addSource(new YjxxtCustomSourceRich("data/secret.txt")).setParallelism(7);
        //转换数据+输出数据
        source.map(new RichMapFunction<String, String>() {

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                // System.out.println("Hello04SourceCustomRich.setRuntimeContext" + System.currentTimeMillis());
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                // System.out.println("Hello04SourceCustomRich.getRuntimeContext" + System.currentTimeMillis());
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                // System.out.println("Hello04SourceCustomRich.getIterationRuntimeContext" + System.currentTimeMillis());
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                // System.out.println("Hello04SourceCustomRich.open" + System.currentTimeMillis());
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                // System.out.println("Hello04SourceCustomRich.close" + System.currentTimeMillis());
                super.close();
            }

            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase() + ":" + getRuntimeContext().getIndexOfThisSubtask();
            }
        }).setParallelism(2).print();

        //这里的setParallelism一般是不需要去主动设置，这个方法主要是给系统用的。

        //运行环境
        environment.execute();

    }
}

class YjxxtCustomSourceRich extends RichParallelSourceFunction<String> {

    private String filePath;
    private int numberOfParallelSubtasks;//任务的总并行度
    private int indexOfThisSubtask;//当前任务的并行度

    public YjxxtCustomSourceRich(String filePath) {
        this.filePath = filePath;
    }

    @Override
    //从写open方法，RichMapFunction被执行时首先会调用该方法，类似与Junit中的beffor注解
    public void open(Configuration parameters) throws Exception {
        this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        System.out.println("YjxxtCustomSourceRich.open任务的并行度为[" + numberOfParallelSubtasks + "]当前任务的并行度编号为[" + indexOfThisSubtask + "]");
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //读取数据文件
        List<String> lines = FileUtils.readLines(new File(filePath), "utf-8");
        //遍历数据
        for (String line : lines) {
            if (Math.abs(line.hashCode()) % numberOfParallelSubtasks == indexOfThisSubtask) {//不管并行度设置为多少，对应的读取数据多少次，这里都只收集一次
                ctx.collect(DESUtil.decrypt("yjxxt0523", line) + "[index" + indexOfThisSubtask + "]");
            }
        }
    }

    @Override
    public void cancel() {

    }
}