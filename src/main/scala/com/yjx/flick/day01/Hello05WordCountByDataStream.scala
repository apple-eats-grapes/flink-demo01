package com.yjx.flick.day01

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Hello05WordCountByDataStream {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment
      .socketTextStream("localhost",19523)
      .flatMap(_.split(" ")).map((_,1)).keyBy(_._1).sum(1).print()

    environment.execute()
  }

}
