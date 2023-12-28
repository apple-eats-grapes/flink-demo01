package com.yjx.flick.day01

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}

object Hello02WordCountByDataSetUseDatesetByscala {
  def main(args: Array[String]): Unit = {
    //创建环境
    val environment = ExecutionEnvironment.getExecutionEnvironment
    //读取数据
    val source = environment.readTextFile("data/wordcount.txt")
    //开始转换
    val flatMapOperator = source.flatMap(_.split(" "))
    val mapOperator = flatMapOperator.map((_, 1))
    val sum = mapOperator.groupBy(0).sum(1)

    //开始计数
    sum.print()

    source.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()

  }

}
