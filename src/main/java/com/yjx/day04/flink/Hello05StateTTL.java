package com.yjx.day04.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
public class Hello05StateTTL {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        environment.enableCheckpointing(5000);
        environment.getCheckpointConfig().setCheckpointStorage("file:///" + System.getProperty("user.dir") + File.separator + "ckpt");
        //获取数据源
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //转换并输出
        // source.map(word -> word.toUpperCase()).print();
        //转换需要添加当前SubTask处理这个单词的序号并输出
        source.map(new YjxxtStateTTLFunction()).print();
        //运行环境
        environment.execute();
    }
}

class YjxxtStateTTLFunction implements MapFunction<String, String>, CheckpointedFunction {

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
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //清除一下历史数据
        countListState.clear();
        //保存数据
        countListState.add(count);
        // System.out.println("YjxxtOperatorStateFunction.snapshotState[" + countListState + "][" + System.currentTimeMillis() + "]");
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(60))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                .cleanupFullSnapshot()
                .build();

        //创建对象的描述器
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>("CountListState", Types.INT);
        //设置状态的TTL
        descriptor.enableTimeToLive(stateTtlConfig);

        //创建对象
        this.countListState = context.getOperatorStateStore().getListState(descriptor);
    }
}