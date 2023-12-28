package com.yjx.day01;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import scala.Tuple2;

public class Hello08TypeInformationCreate {
    public static void main(String[] args) {
        /*
        * 因为JVM运行的时候会擦除类型（泛型），所以flink无法准确的获取数据类型，
        * 所以需要我们对后续得到的数据类型进行指定。指定数据类型共有三种方法，一般用第三种方法较为简便
        *
        * */

        //第一种方式
        TypeInformation<String> of = TypeInformation.of(String.class);//获取到的对象为string类型

        //第二中方式
        TypeInformation.of(new TypeHint<Tuple2<String ,Integer>>() {
        });//返回的对象不是普通类型，需要特殊指定，这里为包含两个元素的元组，元组中的参数类型也可以设定。

        //第三种
        TypeInformation<String> string = Types.STRING;
        TypeInformation<Tuple> tuple = Types.TUPLE(Types.STRING, Types.LONG);//直接通过Type的类型指定得到的数据类型


        /*
        * 在Hello09WordCountTypes中有具体举例
        *
        * */
    }
}
