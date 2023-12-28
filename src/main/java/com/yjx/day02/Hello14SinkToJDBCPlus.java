package com.yjx.day02;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Hello14SinkToJDBCPlus {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);

        //操作数据
        DataStreamSource<String> source = environment.socketTextStream("localhost", 19523);

        SingleOutputStreamOperator<Tuple3<String, String, String>> mapStream = source.map(word -> {
            String[] split = word.split(":");
            return Tuple3.of(split[0], split[1], String.valueOf(System.currentTimeMillis()));
        }, Types.TUPLE(Types.STRING, Types.STRING, Types.STRING));

        //写出数据
        mapStream.addSink(JdbcSink.sink(
                "insert into t_bullet_chat (id, username, msg, ts) values (?,?,?,?)",
                (preparedStatement, tuple3) -> {
                    preparedStatement.setString(1, RandomStringUtils.randomAlphabetic(8));
                    preparedStatement.setString(2, tuple3.f0);
                    preparedStatement.setString(3, tuple3.f1);
                    preparedStatement.setString(4, tuple3.f2);
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(2)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/scott?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));
        //运行环境
        environment.execute();
    }
}
