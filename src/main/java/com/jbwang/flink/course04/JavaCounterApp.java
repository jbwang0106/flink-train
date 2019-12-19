package com.jbwang.flink.course04;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class JavaCounterApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = environment.fromElements("hadoop", "spark", "flink", "pyspark", "storm");

        MapOperator<String, String> info = data.map(new RichMapFunction<String, String>() {

            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("ele-counts-java", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        info.writeAsText("file:///D:/BaiduNetdiskDownload/20-flink/sink-out/counter-java", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(3);

        JobExecutionResult jobResult = environment.execute("java Counter APP");
        Long num = jobResult.getAccumulatorResult("ele-counts-java");

        System.out.println("num = " + num);

    }
}
