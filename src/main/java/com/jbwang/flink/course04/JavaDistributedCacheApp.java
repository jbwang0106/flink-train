package com.jbwang.flink.course04;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

/**
 * @author jbwang0106
 */
public class JavaDistributedCacheApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.registerCachedFile("file:///D:/BaiduNetdiskDownload/20-flink/input/hello.txt", "jbwang-java-dc");

        DataSource<String> data = executionEnvironment.fromElements("hadoop");

        data.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {

                File file = getRuntimeContext().getDistributedCache().getFile("jbwang-java-dc");

                List<String> lines = FileUtils.readLines(file);
                for (String line : lines) {
                    System.out.println(line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
    }
}
