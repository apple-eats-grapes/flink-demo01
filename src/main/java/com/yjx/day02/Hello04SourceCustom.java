package com.yjx.day02;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import com.yjx.util.DESUtil;

import java.io.File;
import java.util.List;

public class Hello04SourceCustom {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<String> source = environment.addSource(new YjxxtCustomSource("data/secret.txt")).setParallelism(1);

        //直接转换并打印
        source.print().setParallelism(1);
        //运行环境
        environment.execute();
    }

}


class YjxxtCustomSource implements ParallelSourceFunction<String>{

    private String filePath;

    public YjxxtCustomSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        //读取数据文件
        List<String> lines = FileUtils.readLines(new File(filePath), "UTF-8");
        //遍历数据
        for (String line:lines){
            sourceContext.collect(DESUtil.decrypt("yjxxt0523",line));
        }
    }

    @Override
    public void cancel() {

    }
}