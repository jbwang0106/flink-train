package com.jbwang.flink.course04

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object DistributedCacheApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        // 注册一个分布式cache
        env.registerCachedFile("file:///D:/BaiduNetdiskDownload/20-flink/input/hello.txt", "jbwang-scala-dc")

        import org.apache.flink.api.scala._
        val data = env.fromElements("hadoop", "spark", "flink", "storm")

        data.map(new RichMapFunction[String, String] {

            override def open(parameters: Configuration): Unit = {
                val file = getRuntimeContext.getDistributedCache.getFile("jbwang-scala-dc")

                val list = FileUtils.readLines(file)
                import scala.collection.JavaConverters._
                // 需要将java的集合转换成scala的集合
                for (ele <- list.asScala) {
                    println(ele)
                }
            }

            override def map(value: String): String = value
        }).print()

       // env.execute("distributed cache app")

    }

}
