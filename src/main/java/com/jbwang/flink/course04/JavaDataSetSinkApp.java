package com.jbwang.flink.course04;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jbwang0106
 */
public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> data = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            data.add(i + 1);
        }

        DataSource<Integer> text = environment.fromCollection(data);

        text.writeAsText("file:///D:/BaiduNetdiskDownload/20-flink/sink-out", FileSystem.WriteMode.OVERWRITE)
        .setParallelism(3);

        environment.execute("Java Data Set Sink APP");


    }
}
