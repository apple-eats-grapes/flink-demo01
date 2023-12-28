package com.yjx.SQL;

import com.yjx.pojo.Emp;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Hello02APIDDL {
    public static void main(String[] args) {

        //创建表环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(environment);

    /*    //获取数据源
        DataStreamSource<String> deptSource = environment.readTextFile("data/dept.txt");
        //创建表：方案一 流表转换
        Table table = streamTableEnvironment.fromDataStream(deptSource);
        table.select(Expressions.$("*")).execute().print();
//        deptTable.execute().print();
*/

        //创建表：方案二 TableAPI
        DataStreamSource<String> empSource = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> map = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
/*        Table table1 = streamTableEnvironment.fromDataStream(map);

        //筛选job为clerk的数据且只显示两列
        table1.where(Expressions.$("job").isEqual("CLERK"))
                .select(Expressions.$("empno"),Expressions.$("job"))
                .execute()
//                .collect()//可以将数据收集
                .print();
//        table1.execute().print();*/

        //创建视图
        streamTableEnvironment.createTemporaryView("v_emp",map);
        streamTableEnvironment.executeSql("select* from v_emp where job= 'CLERK' ").print();



    }
}
