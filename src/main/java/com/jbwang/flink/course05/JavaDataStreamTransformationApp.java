package com.jbwang.flink.course05;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jbwang0106
 */
public class JavaDataStreamTransformationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        filterFunction(environment);
//        unionFunction(environment);
        splitSelectFunction(environment);

        environment.execute("JavaDataStreamTransformationApp");
    }

    private static void splitSelectFunction(StreamExecutionEnvironment environment) {
        DataStreamSource<Long> data = environment.addSource(new JavaCustomNonParallelSourceFunction());

        data.split((OutputSelector<Long>) value -> {
            List<String> list = new ArrayList<>();
            if (value % 2 == 0) {
                list.add("even");
            } else {
                list.add("odd");
            }

            return list;
        }).select("odd")
                .print()
                .setParallelism(1);
    }

    private static void unionFunction(StreamExecutionEnvironment environment) {
        DataStreamSource<Long> data1 = environment.addSource(new JavaCustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = environment.addSource(new JavaCustomNonParallelSourceFunction());

        data1.union(data2)
                .print()
                .setParallelism(1);
    }

    private static void filterFunction(StreamExecutionEnvironment environment) {
        environment.addSource(new JavaCustomNonParallelSourceFunction())
                .map(x -> {
                    System.out.println("received: " + x);
                    return x;
        }).filter(x -> x % 2 == 0)
                .print()
                .setParallelism(1);

    }
}
