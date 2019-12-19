package com.jbwang.flink.course04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * @author jbwang0106
 */
public class JavaBroadCastVarApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 广播订单变量
        DataSource<Integer> toBroadCast = env.fromElements(1, 2, 3);

        DataSource<String> data = env.fromElements("a", "b");

        data.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取广播的变量
                List<Object> broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
                for (Object b : broadcastSet) {
                    System.out.println("broadcast = " + b);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        })
                // 广播这个变量
        .withBroadcastSet(toBroadCast, "broadcastSetName")
                .print();
    }
}
