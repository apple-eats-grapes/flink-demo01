package com.yjx.day05_06.flink;


import com.yjx.pojo.Emp;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello03APIDataTypes {
    public static void main(String[] args) {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //Tuple类型
        DataStreamSource<String> tupleSource = environment.fromElements("aa,11", "bb,22", "cc,33");
        DataStream<Tuple2<String, String>> tupleStream = tupleSource.map(word -> {
            String[] split = word.split(",");
            return Tuple2.of(split[0], split[1]);
        }, Types.TUPLE(Types.STRING, Types.STRING));
        Table tupleTable = tableEnvironment.fromDataStream(tupleStream);
        tupleTable.execute().print();

        //Pojo类型
        DataStreamSource<String> empSource = environment.readTextFile("data/emp.txt");
        DataStream<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table empTable = tableEnvironment.fromDataStream(empStream);
        empTable.execute().print();

        //Row类型
        DataStream<Row> dataStream = environment.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));
        Table rowTable = tableEnvironment.fromChangelogStream(dataStream);
        rowTable.execute().print();
    }
}
