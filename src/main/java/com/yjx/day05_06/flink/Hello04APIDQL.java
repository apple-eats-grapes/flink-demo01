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
public class Hello04APIDQL {
    public static void main(String[] args) {
        //运行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        //Pojo类型
        DataStreamSource<String> empSource = environment.readTextFile("data/emp.txt");
        DataStream<Emp> empStream = empSource.map(line -> new ObjectMapper().readValue(line, Emp.class));
        Table empTable = tableEnvironment.fromDataStream(empStream);

        //查询数据：TableAPI--查询20部门员工信息的 编号 姓名 薪资 部门编号
        empTable.filter($("deptno").isEqual(20))
                .select($("empno"), $("ename"), $("sal"), $("deptno"))
                .execute()
                .print();

        //查询数据：SQL--查询20部门员工信息的 编号 姓名 薪资 部门编号
        tableEnvironment.createTemporaryView("t_emp", empTable);
        tableEnvironment.sqlQuery("select empno,ename,sal,deptno from t_emp where deptno = 20")
                .execute()
                .print();

        //查询数据：混合方式--查询20部门员工信息的 编号 姓名 薪资 部门编号
        //SQL查询中可以直接使用table的信息
        //SQL查询结果返回的又是一个Table
        tableEnvironment.sqlQuery("select * from " + empTable.toString() + " where deptno = 20")
                .filter($("job").isEqual("ANALYST"))
                .select($("ename"), $("job"))
                .execute()
                .print();

    }
}
