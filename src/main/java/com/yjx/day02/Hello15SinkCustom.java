package com.yjx.day02;


import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.util.ArrayList;

/*
* 自定义输出文件
* */
public class Hello15SinkCustom {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        //操作数据
        ArrayList<String> list = new ArrayList<>();
        list.add("君子周而不比，小人比而不周");
        list.add("君子喻于义，小人喻于利");
        list.add("君子怀德，小人怀土；君子怀刑，小人怀惠");
        list.add("君子欲讷于言而敏于行");
        list.add("君子坦荡荡，小人长戚戚");
        DataStreamSource<String> source = environment.fromCollection(list);

        //写出数据                                                   时间戳保证文件不重名
        source.addSink(new YjxxtCustomSinkRich("data/sink" + System.currentTimeMillis())).setParallelism(1);

        //运行环境
        environment.execute();
    }

}
class YjxxtCustomSink implements SinkFunction<String> {

    private File file;

    public YjxxtCustomSink(String filePath) {
        this.file = new File(filePath);
    }

    @Override
    public void invoke(String line, Context context) throws Exception {
        //加密数据
        String encrypt = DESUtil.encrypt("yjxxt0523", line) + "\r\n";
        //写出数据
        FileUtils.writeStringToFile(file, encrypt, "utf-8", true);//写出数据到文件并且可追加
    }
}

