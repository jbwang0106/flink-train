package com.jbwang.flink.project

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.slf4j.LoggerFactory

object LogAnalysis {

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


        // 设置一个水印，允许的最大乱序时间
        val analysisData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {

            val maxOutOfOrderness = 10000L // 3.5 seconds

            var currentMaxTimestamp: Long = _

            override def getCurrentWatermark: Watermark = {
                new Watermark(currentMaxTimestamp - maxOutOfOrderness)
            }

            override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
                val timestamp = element._1
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
                timestamp
            }
        }).keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .apply(new WindowFunction[(Long, String, Long), (String, String, Long), Tuple, TimeWindow] {

                    override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {

                        val domain = key.getField(0).toString
                        var sum = 0L
                        var time = ""
                        val sdf = new SimpleDateFormat(("yyyy-MM-dd HH:mm:ss"))

                        val iterator = input.iterator
                        while (iterator.hasNext) {
                            val next = iterator.next()
                            sum += next._3
                            time = sdf.format(new Date(next._1))
                            time = time.substring(0, time.length - 3)
                        }

                        out.collect((time, domain, sum))
                    }

                }) // .print().setParallelism(1)

        // 添加sink
        val httpHosts = new java.util.ArrayList[HttpHost]
        httpHosts.add(new HttpHost("172.32.2.95", 9200, "http"))
        httpHosts.add(new HttpHost("172.32.2.251", 9200, "http"))
        val esSinkBuilder = new ElasticsearchSink.Builder[(String, String, Long)](
            httpHosts,
            new ElasticsearchSinkFunction[(String, String, Long)] {
                def createIndexRequest(element: (String, String, Long)): IndexRequest = {
                    val json = new java.util.HashMap[String, Any]
                    json.put("time", element._1)
                    json.put("domain", element._2)
                    json.put("traffics", element._3)

                    val id = element._1 + "-" + element._3
                    return Requests.indexRequest()
                        .index("cdn")
                        .`type`("traffic")
                            .id(id)
                        .source(json)
                }

                override def process(t: (String, String, Long), runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
                    requestIndexer.add(createIndexRequest(t))
                }
            }
        )

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1)

        // finally, build and add the sink to the job's pipeline
        analysisData.addSink(esSinkBuilder.build)

        env.execute("log analysis")

    }

}
