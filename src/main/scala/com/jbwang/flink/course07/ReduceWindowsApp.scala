package com.jbwang.flink.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object ReduceWindowsApp {

    def main(args: Array[String]): Unit = {
        import org.apache.flink.api.scala._

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val text = env.socketTextStream("172.32.2.42", 9999)

        text.flatMap(_.split(","))
                .map(x => (1, x.toInt))
                // .map((_, 1))
                .keyBy(0)
                .timeWindow(Time.seconds(5))
              //  .timeWindow(Time.seconds(10), Time.seconds(5))
              //  .sum(1)
                .reduce((v1, v2) => {
                    (v1._1, v1._2 + v2._2)
                })
                .print()
                .setParallelism(1)

        env.execute("reduce windows app")
    }

}
