package com.yjx.day02;

import com.yjx.util.DESUtil;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.File;
import java.util.ArrayList;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello16SinkCustomRich {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(4);
        //操作数据
        ArrayList<String> list = new ArrayList<>();
        list.add("君子周而不比，小人比而不周");
        list.add("君子喻于义，小人喻于利");
        list.add("君子怀德，小人怀土；君子怀刑，小人怀惠");
        list.add("君子欲讷于言而敏于行");
        list.add("君子坦荡荡，小人长戚戚");
        DataStreamSource<String> source = environment.fromCollection(list);

        //写出数据
        source.addSink(new YjxxtCustomSinkRich("data/sink" + System.currentTimeMillis()));

        //运行环境
        environment.execute();
    }
}

class YjxxtCustomSinkRich extends RichSinkFunction<String> {

    private File file;
    private String filePath;
    private int indexOfThisSubtask;
    private int numberOfParallelSubtasks;

    public YjxxtCustomSinkRich(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //获取当前任务的总并行度
        this.numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        //获取当前任务的并行度
        this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        //获取当前的写出路径
        this.file = new File(this.filePath + File.separator + indexOfThisSubtask);
    }

    @Override
    public void invoke(String line, Context context) throws Exception {
        //加密数据
        String encrypt = DESUtil.encrypt("yjxxt0523", line) + "\r\n";
        //写出数据
        FileUtils.writeStringToFile(file, encrypt, "utf-8", true);
    }
}
