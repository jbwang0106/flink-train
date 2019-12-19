package com.jbwang.flink.course04

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

object CounterApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        import org.apache.flink.api.scala._
        val data = env.fromElements("hadoop", "spark", "flink", "pyspark", "storm")

//        data.map(new RichMapFunction[String, Int] {
//            var counter = 0
//            override def map(in: String): Int = {
//                counter = counter + 1
//                println("counter: " + counter)
//                counter
//            }
//        }).setParallelism(5)
//            .print()

        val info = data.map(new RichMapFunction[String, String] {

            // 1. 注册一个计数器
            val counter = new LongCounter()

            override def open(parameters: Configuration): Unit = {
                // 注册一个计数器
                getRuntimeContext.addAccumulator("ele-counts-scala", counter)
            }

            override def map(in: String): String = {
                counter.add(1)
                in
            }
        })

        val filePath = "file:///D:/BaiduNetdiskDownload/20-flink/sink-out/counter-scala"

        info.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(2)

        val result = env.execute("CounterApp").getAccumulatorResult[Long]("ele-counts-scala")

        println("num = " + result)
    }

}
