package com.yjx.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Description :
 * @School:优极限学堂
 * @Official-Website: http://www.yjxxt.com
 * @Teacher:李毅大帝
 * @Mail:863159469@qq.com
 */
public class ScalarFunction4DataGen2RandomStr extends ScalarFunction {

    // 接受任意类型输入，返回 INT 型输出
    public String eval(String input) {
        return input.concat(String.valueOf(input.length()));
    }

}
