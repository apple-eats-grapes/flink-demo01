package com.yjx.day02;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;

/*
* 写出到到mysql中
* */
public class Hello13SinkToJDBC {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        ArrayList<Tuple3<String ,String,String >> list = new ArrayList<>();

        list.add(Tuple3.of("张三1","good1",String.valueOf(System.currentTimeMillis())));
        list.add(Tuple3.of("张三2", "good2", String.valueOf(System.currentTimeMillis())));
        list.add(Tuple3.of("张三1", "good3", String.valueOf(System.currentTimeMillis())));
        list.add(Tuple3.of("张三2", "good4", String.valueOf(System.currentTimeMillis())));
        DataStreamSource<Tuple3<String, String, String>> source = environment.fromCollection(list);

        source.addSink(JdbcSink.sink(
                "insert into t_bullet_chat (id, username, msg, ts) values (?,?,?,?)",
                (preparedStatement, tuple3) -> {
                    preparedStatement.setString(1, RandomStringUtils.randomAlphabetic(8));
                    preparedStatement.setString(2, tuple3.f0);
                    preparedStatement.setString(3, tuple3.f1);
                    preparedStatement.setString(4, tuple3.f2);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/scott?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()
        ));

        environment.execute();

    }
}
