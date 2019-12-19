package com.jbwang.flink.course04

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BroadcastVarApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        import org.apache.flink.api.scala._

        val toBroadcast = env.fromElements(1, 2, 3)
        val data = env.fromElements("a", "b")

        data.map(new RichMapFunction[String, String] {

            override def open(parameters: Configuration): Unit = {
                import scala.collection.JavaConverters._
                val broadcastSetName = getRuntimeContext.getBroadcastVariable[Int]("scalaBroadcast")

                broadcastSetName.asScala.foreach(println(_))
            }

            override def map(value: String): String = value

        }).withBroadcastSet(toBroadcast, "scalaBroadcast")
            .print()

    }

}
