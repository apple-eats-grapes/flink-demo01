package com.yjx.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */

@FunctionHint(output = @DataTypeHint("ROW<type STRING, score INT>"))
public class TableFunction4Movie2Types extends TableFunction<Row> {
    /**
     * 喜剧:8_动画:7_冒险:3_音乐:9_家庭:6
     *
     * @param types
     */
    public void eval(String types) {
        for (String type : types.split("_")) {
            String[] split = type.split(":");
            collect(Row.of(split[0], Integer.parseInt(split[1])));
        }
    }
}
