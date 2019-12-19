package com.jbwang.flink.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author jbwang0106
 */
public class JavaDataSetDataSourceApp {

    public static void main(String[] args) throws Exception {

        // 1. 获取执行的上下文环境  get execution context
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        fromCollection(env);

//        testFile(env);

        // testCsvFile(env);
        readRecursiveFiles(env);

    }

    /**
     * from recursive file path
     * @param env
     * @throws Exception
     */
    public static void readRecursiveFiles(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///D:/BaiduNetdiskDownload/20-flink/inputjoin";
        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration", true);
        env.readTextFile(filePath).withParameters(configuration).print();
    }

    /**
     * from csv file path
     * @param env
     * @throws Exception
     */
    public static void testCsvFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///D:/BaiduNetdiskDownload/20-flink/input/hello.csv";
        env.readCsvFile(filePath)
                .fieldDelimiter(",")
                .ignoreFirstLine()
                .pojoType(Person.class, "name", "age", "work")
                .print();
    }

    /**
     * from file path
     * @param env
     * @throws Exception
     */
    public static void testFile(ExecutionEnvironment env) throws Exception {
        String filePath = "file:///D:/BaiduNetdiskDownload/20-flink/inputformat";
        env.readTextFile(filePath).print();
    }

    /**
     * from collection
     * @param env
     * @throws Exception
     */
    public static void fromCollection(ExecutionEnvironment env) throws Exception {
        List<Integer> list = Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .collect(Collectors.toList());

        List<Integer> collect = Stream.iterate(0, n -> n + 2)
                .limit(10)
                .collect(Collectors.toList());
        System.out.println(collect);

        // 从集合读取数据并打印到控制台
        env.fromCollection(list).print();
    }
}
