package com.yjx.flick.day01

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Hello04WordCountUseDataStreamByScala{
  def main(args: Array[String]): Unit = {
    //执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    //source
    val source = environment.socketTextStream("localhost", 19523)
    //transformation
    source.flatMap(_.split(" " )).map((_,1)).keyBy(_._1).sum(1 ).print()
    //运行环境
    environment.execute()
  }

}
