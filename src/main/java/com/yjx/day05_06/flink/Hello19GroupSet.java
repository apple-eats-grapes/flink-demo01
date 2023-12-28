package com.yjx.day05_06.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class Hello19GroupSet {
    public static void main(String[] args) {
        //批处理运行环境
        TableEnvironment tableEnvironment = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        //执行SQL
        // tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
        //         "FROM (VALUES\n" +
        //         " ('省1','市1','县1',100),\n" +
        //         " ('省1','市2','县2',101),\n" +
        //         " ('省1','市2','县1',102),\n" +
        //         " ('省2','市1','县4',103),\n" +
        //         " ('省2','市2','县1',104),\n" +
        //         " ('省2','市2','县1',105),\n" +
        //         " ('省3','市1','县1',106),\n" +
        //         " ('省3','市2','县1',107),\n" +
        //         " ('省3','市2','县2',108),\n" +
        //         " ('省4','市1','县1',109),\n" +
        //         " ('省4','市2','县1',110))\n" +
        //         "AS t_person_num(pid, cid, xid, num)\n" +
        //         "GROUP BY GROUPING SETS ((pid, cid, xid),(pid, cid),(pid), ())").execute().print();

        //执行SQL
        tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
                "FROM (VALUES\n" +
                " ('省1','市1','县1',100),\n" +
                " ('省1','市2','县2',101),\n" +
                " ('省1','市2','县1',102),\n" +
                " ('省2','市1','县4',103),\n" +
                " ('省2','市2','县1',104),\n" +
                " ('省2','市2','县1',105),\n" +
                " ('省3','市1','县1',106),\n" +
                " ('省3','市2','县1',107),\n" +
                " ('省3','市2','县2',108),\n" +
                " ('省4','市1','县1',109),\n" +
                " ('省4','市2','县1',110))\n" +
                "AS t_person_num(pid, cid, xid, num)\n" +
                "GROUP BY ROLLUP(pid, cid, xid)").execute().print();

        //执行SQL
        // tableEnvironment.sqlQuery("SELECT pid, cid, xid, sum(num) AS total\n" +
        //         "FROM (VALUES\n" +
        //         " ('省1','市1','县1',100),\n" +
        //         " ('省1','市2','县2',101),\n" +
        //         " ('省1','市2','县1',102),\n" +
        //         " ('省2','市1','县4',103),\n" +
        //         " ('省2','市2','县1',104),\n" +
        //         " ('省2','市2','县1',105),\n" +
        //         " ('省3','市1','县1',106),\n" +
        //         " ('省3','市2','县1',107),\n" +
        //         " ('省3','市2','县2',108),\n" +
        //         " ('省4','市1','县1',109),\n" +
        //         " ('省4','市2','县1',110))\n" +
        //         "AS t_person_num(pid, cid, xid, num)\n" +
        //         "GROUP BY CUBE(pid, cid, xid)").execute().print();

    }
}
