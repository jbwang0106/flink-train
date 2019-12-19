package com.jbwang.flink.course05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jbwang0106
 */
public class JavaDataStreamSourceApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // socketFunction(environment);
        // nonParallelSourceFunction(environment);
        parallelSourceFunction(environment);

        environment.execute("JavaDataStreamSourceApp");
    }

    private static void parallelSourceFunction(StreamExecutionEnvironment environment) {
        DataStreamSource<Long> data = environment.addSource(new JavaCustomParallelSourceFunction()).setParallelism(3);
        data.print();
    }

    private static void nonParallelSourceFunction(StreamExecutionEnvironment environment) {
        DataStreamSource<Long> data = environment.addSource(new JavaCustomNonParallelSourceFunction());
        data.print().setParallelism(1);
    }

    private static void socketFunction(StreamExecutionEnvironment environment) {
        DataStreamSource<String> data = environment.socketTextStream("172.32.2.42", 9999);
        data.print();
    }

}
