package com.yjx.day04.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

/*
*
* setCheckpointStorage 控制 Flink 的检查点存储位置，是自动创建的用于容错的状态快照。
execution.savepoint.path 控制 Flink 任务保存点的位置，是手动创建的状态快照，通常用于任务的迁移和恢复。
* */
public class Hello02StateOperatorRestore {
    public static void main(String[] args) throws Exception {
        //配置程序执行的参数
        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path", "C:\\Users\\SkyWorth\\IdeaProjects\\flink060104_state\\ckpt\\089483a5f8e62fce081271533b1fcc3f\\chk-10");//设置保存点的名称以及位置，用于手动保存时使用。
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setParallelism(2);
        environment.enableCheckpointing(5000);//设置保存间隔5秒
        environment.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");
        //自动保存状态存储位置，System.getProperty("user.dir")，获取当前工作主目录，user.dir是固定用于获取主目录的，File.separator 为分隔符
        //获取数据源
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //转换并输出
        // source.map(word -> word.toUpperCase()).print();
        //转换需要添加当前SubTask处理这个单词的序号并输出
        source.map(new YjxxtOperatorStateFunctionSRestore()).print();
        //运行环境
        environment.execute();
    }
}
//实现MapFunction是将状态单独记录数据的状态在内存中，实现CheckpointedFunction是为了将内存中的数据持久化到磁盘中，方便数据恢复
class YjxxtOperatorStateFunctionSRestore implements MapFunction<String, String>, CheckpointedFunction {

    //声明一个变量记数
    private int count;
    //创建一个状态对象
    private ListState<Integer> countListState;

    @Override
    public String map(String value) throws Exception {
        //更新计数器
        count++;
        return "[" + value.toUpperCase() + "][" + count + "]";
    }

    @Override
    //对数据做快照处理
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //清除一下历史数据
        countListState.clear();
        //保存数据
        countListState.add(count);
        // System.out.println("YjxxtOperatorStateFunction.snapshotState[" + countListState + "][" + System.currentTimeMillis() + "]");
    }

    @Override
    //初始化快照
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //创建对象的描述器
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>("CountListState", Types.INT);
        //创建对象
        this.countListState = context.getOperatorStateStore().getListState(descriptor);
        //判断是否恢复成功
        if (context.isRestored()) {
            System.out.println("YjxxtOperatorStateFunction.initializeState【历史状态恢复成功】");
            for (Integer integer : countListState.get()) {
                this.count = integer;
            }
        } else {
            System.out.println("YjxxtOperatorStateFunction.initializeState【历史状态恢复失败】");
        }
    }
}







