package com.jbwang.flink.course05

import java.lang

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer

object DataStreamTransformationApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // filterFunction(env)
        // unionFunction(env)
        splitSelectFunction(env)

        env.execute("DataStreamTransformationApp")
    }

    def splitSelectFunction(environment: StreamExecutionEnvironment): Unit = {

        val data = environment.addSource(new CustomNonParallelSourceFunction)

        data.split(new OutputSelector[Long] {
            override def select(value: Long): lang.Iterable[String] = {
                val outputSelector = new java.util.ArrayList[String]
                if (value % 2 == 0) {
                    outputSelector.add("even")
                } else {
                    outputSelector.add("odd")
                }
                outputSelector
            }
        }).select("odd")
            .print()
            .setParallelism(1)
    }

    def unionFunction(environment: StreamExecutionEnvironment): Unit = {
        val data1 = environment.addSource(new CustomNonParallelSourceFunction)
        val data2 = environment.addSource(new CustomNonParallelSourceFunction)

        data1.union(data2).print().setParallelism(1)
    }

    def filterFunction(environment: StreamExecutionEnvironment): Unit = {
        environment.addSource(new CustomNonParallelSourceFunction)
            .map(x => {
                println("received: " + x)
                x
            }).filter(_ % 2 == 0)
            .print()
            .setParallelism(1)
    }
}
