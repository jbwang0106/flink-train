package com.jbwang.flink.project;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @author jbwang0106
 * MOck数据，往kafka发送信息
 */
public class MockKafkaProducer {

    public static void main(String[] args) {

        //创建一个kafka producer
        // 1.1指定相关的参数
        Properties kafkaProps = new Properties();
        //指定broker的地址清单，并不需要包含所有的broker地址，生产者会自动去找到其他的broker信息
        kafkaProps.put("bootstrap.servers", "172.32.2.95:9092");
        //设置key和value的序列化器
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 1.2 创建producer对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);
        String topic = "jbl-topic";

        while (true) {
            // 2. 创建一个消息记录并进行发送
            StringBuilder builder = new StringBuilder();
            builder.append("imooc").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");

            System.out.println("====" + builder.toString());
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, builder.toString());
            try {
                // 2.1同步发送消息
                producer.send(record);
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private static long getTraffic() {
        return new Random().nextInt(10000);
    }

    private static String getDomains() {
        String[] domains = new String[] {
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com"
        };

        return domains[new Random().nextInt(domains.length)];
    }

    private static String getIps() {
        String[] ips = new String[] {
                "117.28.38.28",
                "117.59.39.169",
                "59.83.198.84",
                "183.227.58.21",
                "112.1.66.34",
                "175.148.211.190",
                "27.17.127.135",
                "113.101.75.194",
                "172.32.2.41",
                "172.32.2.42",
                "172.32.2.95",
                "223.104.18.110"
        };
        return ips[new Random().nextInt(ips.length)];
    }

    private static String getLevels() {
        String[] levels = new String[]{"E", "M"};
        return levels[new Random().nextInt(levels.length)];
    }
}
