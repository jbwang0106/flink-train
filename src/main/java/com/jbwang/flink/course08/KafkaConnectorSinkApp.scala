package com.jbwang.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaConnectorSinkApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val data = env.socketTextStream("172.32.2.42", 9999)

        val topic = "jb-test"
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "172.32.2.95:9092")

        val sink = new FlinkKafkaProducer[String](topic, new SimpleStringSchema, properties)

        data.addSink(sink)

        env.execute("KafkaConnectorSinkApp")
    }

}
