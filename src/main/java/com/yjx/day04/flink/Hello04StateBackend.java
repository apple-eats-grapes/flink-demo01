package com.yjx.day04.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello04StateBackend {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        environment.enableCheckpointing(5000);
        //本地状态维护
        environment.setStateBackend(new EmbeddedRocksDBStateBackend());
        //远程状态备份
        environment.getCheckpointConfig().setCheckpointStorage("hdfs://node02:8020/flink/checkpoints");
        //获取Source
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);
        //转换数据
        source.map(new YjxxtStateBackendFunction()).print();
        //运行环境
        environment.execute();
    }
}

class YjxxtStateBackendFunction implements MapFunction<String, String>, CheckpointedFunction {

    //声明变量计数器
    private int count;
    //状态对象
    private ListState<Integer> listState;

    @Override
    public String map(String value) throws Exception {
        //计数器累加
        count++;
        return "[" + value.toUpperCase() + "][" + count + "]";
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        //清空并更新数据
        listState.clear();
        listState.add(count);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //创建描述器并创建对象
        ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<>("ListState", Types.INT);
        this.listState = context.getOperatorStateStore().getListState(descriptor);
    }

}
