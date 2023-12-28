package com.yjx.SQL;

import com.yjx.pojo.Emp;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTemporalJoin;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import static org.apache.flink.table.api.Expressions.$;

public class Hello03APIDDL {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        DataStreamSource<String> source = environment.readTextFile("data/emp.txt");
        SingleOutputStreamOperator<Emp> map = source.map(line -> new ObjectMapper().readValue(line, Emp.class));

        Table table = tableEnvironment.fromDataStream(map);

        table.where($("job").isEqual("CLERK"))
                .groupBy("deptno")
                .select($("sal").max(), $("deptno")).execute().print();


        Table select = table.where($("job").isEqual("CLERK"))
                .groupBy("deptno")
                .select($("sal").max(), $("deptno"));//直接转换成表
        System.out.println(select.explain());//打印执行计划



        tableEnvironment.createTemporaryView("v_emp",map);
//        CloseableIterator<Row> collect = tableEnvironment.sqlQuery("select * from v_emp").execute().collect();//这里使用sqlQuery运行sql后的到的结果可以直接打印，也可以直接收集。
//        tableEnvironment.sqlQuery("select * from v_emp").execute().print();
        tableEnvironment.executeSql("select sum(sal) as sum_sal,deptno from v_emp where job = 'CLERK' group by deptno  ").print();//executeSql直接打印


        //查询混用
        tableEnvironment.sqlQuery("select * from " + table.toString()).execute().print();//这里的toString（）与 pojoj类中的对象中的toString（）方法没有关系。

//        DataStream<Row> rowDataStream = tableEnvironment.toChangelogStream(table);//将表直接转成流
//        rowDataStream.map()

    }
}
