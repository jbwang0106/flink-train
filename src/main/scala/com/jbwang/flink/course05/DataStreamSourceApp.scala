package com.jbwang.flink.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.api.scala._

object DataStreamSourceApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // socketFunction(env)
       // nonParallelSourceFunction(env)
       // parallelSourceFunction(env)
        richParallelSourceFunction(env)

        env.execute("DataStreamSourceApp")

    }

    def richParallelSourceFunction(environment: StreamExecutionEnvironment): Unit = {
        val data = environment.addSource(new CustomRichParallelSourceFunction).setParallelism(6)
        data.print()
    }

    def parallelSourceFunction(environment: StreamExecutionEnvironment): Unit = {
        val data = environment.addSource(new CustomParallelSourceFunction).setParallelism(6)
        data.print()
    }

    def nonParallelSourceFunction(environment: StreamExecutionEnvironment): Unit = {
        val data = environment.addSource(new CustomNonParallelSourceFunction).setParallelism(1)
        data.print().setParallelism(1)
    }

    def socketFunction(environment: StreamExecutionEnvironment): Unit = {
        val data = environment.socketTextStream("172.32.2.42", 9999)
        data.print().setParallelism(1)
    }

}
