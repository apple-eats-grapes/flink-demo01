package com.yjx.day05_06.flink;

import com.yjx.pojo.Emp;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello05APISink {
    public static void main(String[] args) throws Exception {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //Pojo类型
        DataStreamSource<String> empSource = environment.readTextFile("data/emp.txt");
        DataStream<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table empTable = tableEnvironment.fromDataStream(empStream);

        //TableApi查询的结果为table
        Table table1 = empTable.select($("empno"), $("ename"));
        //SQL查询的结果为table
        Table table2 = tableEnvironment.sqlQuery("select empno,ename,job from " + empTable.toString());

        //方案1：将表转成流
        tableEnvironment.toDataStream(table1).print();

        //方案2：连接器
        tableEnvironment.executeSql(
                "CREATE TABLE print_table (\n" +
                        " empno INT,\n" +
                        " ename STRING,\n" +
                        " job STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'print'\n" +
                        ")");
        table2.insertInto("print_table").execute();

        //执行环境
        environment.execute();

    }
}
