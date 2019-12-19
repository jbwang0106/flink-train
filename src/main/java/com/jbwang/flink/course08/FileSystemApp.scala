package com.jbwang.flink.course08

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object FileSystemApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val data = env.socketTextStream("172.32.2.42", 9999)

        data.print().setParallelism(1)

        // add sink
        val sink = new BucketingSink[String]("D:\\BaiduNetdiskDownload\\20-flink\\compression")
        sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
        sink.setWriter(new StringWriter())
       // sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
        sink.setBatchRolloverInterval(20); // this is 20 mins

        data.addSink(sink)

        env.execute("file system app")

    }

}
