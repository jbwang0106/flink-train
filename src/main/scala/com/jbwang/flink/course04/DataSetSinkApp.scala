package com.jbwang.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

object DataSetSinkApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        val text = env.fromCollection(1 to 10)

        val filePath = "file:///D:/BaiduNetdiskDownload/20-flink/sink-out"

        text.writeAsText(filePath, WriteMode.OVERWRITE).setParallelism(2)

        env.execute("DataSet Sink App")

    }

}
