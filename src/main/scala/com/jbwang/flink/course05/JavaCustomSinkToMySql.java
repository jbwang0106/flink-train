package com.jbwang.flink.course05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author jbwang0106
 */
public class JavaCustomSinkToMySql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.socketTextStream("172.32.2.42", 9999);

        SingleOutputStreamOperator<Student> studentStream = data.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {

                Student student = new Student();

                String[] split = value.split(",");
                student.setId(Integer.valueOf(split[0]));
                student.setName(split[1]);
                student.setAge(Integer.valueOf(split[2]));

                return student;
            }
        });

        studentStream.addSink(new SinkToMySql());

        env.execute("JavaCustomSinkToMySql");

    }

}
