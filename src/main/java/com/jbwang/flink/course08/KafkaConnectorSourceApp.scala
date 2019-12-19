package com.jbwang.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object KafkaConnectorSourceApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        import org.apache.flink.api.scala._

        val topic = "jb-test"
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "172.32.2.95:9092")
        properties.setProperty("group.id", "jb-test")
        env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(), properties))
                .print()

        env.execute("KafkaConnectorSourceApp")

    }

}
