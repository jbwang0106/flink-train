package com.jbwang.flink.course04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author jbwang0106
 */
public class JavaDataSetDataTransformationApp {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行的上下文环境  get execution context
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

       // mapFunction(env);
       // System.out.println("==========");
      //  filterFunction(env);
       // mapPartitionFunction(env);
       // firstFunction(env);
       // flatMapFunction(env);
      //  distinctFunction(env);

       // joinFunction(env);
       // outerJoinFunction(env);
        crossFunction(env);
    }

    private static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = Arrays.asList("曼联", "曼城");
        List<Integer> info2 = Arrays.asList(3, 1, 0);

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }

    private static void outerJoinFunction(ExecutionEnvironment env) throws Exception {

        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "Flink"));
        info1.add(new Tuple2<>(2, "Spark"));
        info1.add(new Tuple2<>(3, "Hadoop"));
        info1.add(new Tuple2<>(4, "Java"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "成都"));
        info2.add(new Tuple2<>(5, "西安"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

//        data1.leftOuterJoin(data2)
//                .where(0)
//                .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//                    @Override
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                        if (second == null) {
//                            return new Tuple3<>(first.f0, first.f1, "-");
//                        }
//                        return new Tuple3<>(first.f0, first.f1, second.f1);
//                    }
//                }).print();

//        data1.rightOuterJoin(data2)
//                .where(0)
//                .equalTo(0)
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
//                    @Override
//                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                        if (first == null) {
//                            return new Tuple3<>(second.f0, "-", second.f1);
//                        }
//                        return new Tuple3<>(first.f0, first.f1, second.f1);
//                    }
//                }).print();

        data1.fullOuterJoin(data2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return new Tuple3<>(second.f0, "-", second.f1);
                        } else if (second == null) {
                            return new Tuple3<>(first.f0, first.f1, "-");
                        }
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                }).print();
    }

    private static void joinFunction(ExecutionEnvironment env) throws Exception {

        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "Flink"));
        info1.add(new Tuple2<>(2, "Spark"));
        info1.add(new Tuple2<>(3, "Hadoop"));
        info1.add(new Tuple2<>(4, "Java"));

        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info2.add(new Tuple2<>(1, "北京"));
        info2.add(new Tuple2<>(2, "上海"));
        info2.add(new Tuple2<>(3, "成都"));
        info2.add(new Tuple2<>(5, "西安"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                }).print();
    }

    private static void distinctFunction(ExecutionEnvironment env) throws Exception {

        List<String> info = Arrays.asList("hadoop,spark", "hadoop,flink", "spark,flink");
        DataSource<String> data = env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String values, Collector<String> collector) throws Exception {
                String[] tokens = values.split(",");

                for (String token : tokens) {
                    collector.collect(token);
                }
            }
        }).distinct()
                .print();

    }

    private static void flatMapFunction(ExecutionEnvironment env) throws Exception {

        List<String> info = Arrays.asList("hadoop,spark", "hadoop,flink", "spark,flink");
        DataSource<String> data = env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String values, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = values.split(",");

                for (String token : tokens) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }).groupBy(0)
                .sum(1)
                .print();

    }


    private static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<>();
        info.add(new Tuple2<>(1, "Flink"));
        info.add(new Tuple2<>(1, "Spark"));
        info.add(new Tuple2<>(1, "Hadoop"));
        info.add(new Tuple2<>(2, "Java"));
        info.add(new Tuple2<>(2, "Scala"));
        info.add(new Tuple2<>(3, "Linux"));
        info.add(new Tuple2<>(4, "Vue-js"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);

        data.first(3).print();
        System.out.println("~~~~");
        data.groupBy(0).first(2).print();
        System.out.println("~~~~~~");
        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
    }

    public static void mapPartitionFunction(ExecutionEnvironment executionEnvironment) throws Exception {
        List<String> students = Stream.iterate(0, n -> n + 1)
                .map(n -> "student: " + n)
                .limit(100)
                .collect(Collectors.toList());

        DataSource<String> data = executionEnvironment.fromCollection(students).setParallelism(3);

        data.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                String connection = "";//DBUtils.getConnection();
                System.out.println(connection + "....");
                // DBUtils.returnConnection(connection);
                iterable.forEach(collector::collect);
            }
        }).print();
    }

    /**
     * filter function
     * @param env
     * @throws Exception
     */
    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Stream.iterate(0, n -> n + 1)
                .limit(10)
                .collect(Collectors.toList());

        // 从集合读取数据并打印到控制台
        env.fromCollection(list)
                .map(integer -> integer + 1)
                .filter(integer -> integer % 2 == 0)
                .print();
    }

    /**
     * map function
     * @param env
     * @throws Exception
     */
    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .collect(Collectors.toList());

        List<Integer> collect = Stream.iterate(0, n -> n + 2)
                .limit(10)
                .collect(Collectors.toList());

        // 从集合读取数据并打印到控制台
        env.fromCollection(list)
                .map(integer -> integer * 2).print();
    }
}
