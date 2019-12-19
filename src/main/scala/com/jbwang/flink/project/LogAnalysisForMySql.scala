package com.jbwang.flink.project

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

import scala.collection.mutable

object LogAnalysisForMySql {

    val LOGGER = LoggerFactory.getLogger(LogAnalysis.getClass.getSimpleName)

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        // 设置使用event time来进行统计
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val topic = "jbl-topic"
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "172.32.2.95:9092")
        properties.setProperty("group.id", "jbl-group")

        val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

        import org.apache.flink.api.scala._

        // 接受kafka的数据
        val data = env.addSource(consumer)

        // 清洗数据，要把脏数据给过滤掉
        val logData = data.map(x => {
            val splits = x.split("\t")
            val level = splits(2)

            val timeStr = splits(3)
            var time = 0L
            val sdf = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"))

            try {
                time = sdf.parse(timeStr).getTime
            } catch {
                case e: Exception => {
                   LOGGER.error(s"time parse error $timeStr", e.printStackTrace())
                }
            }

            val domain = splits(5)
            val traffic = splits(6).toLong

            (level, time, domain, traffic)
        }).filter(_._2 != 0)
                .filter(_._1.equals("E"))
                .map(x => (x._2, x._3, x._4))

        val mysqlData = env.addSource(new MySqlSource)

        // 连接两个source
        val connectData = logData.connect(mysqlData)
                .flatMap(new CoFlatMapFunction[(Long, String, Long), mutable.HashMap[String, String], String] {

                    var userDomainMap = mutable.HashMap[String, String]()

                    override def flatMap1(value: (Long, String, Long), out: Collector[String]): Unit = {
                        val domain = value._2
                        val userId = userDomainMap.getOrElseUpdate(domain, "")

                      //  LOGGER.info("userId: {}", userId)
                        out.collect(value._1 + "\t" + value._2 + "\t" + value._3 + "\t" + userId)
                    }

                    override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]): Unit = {
                        userDomainMap = value
                    }
                })

        connectData.print().setParallelism(1)

        env.execute("log analysis")

    }

}
