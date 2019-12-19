package com.jbwang.flink.course05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author jbwang0106
 */
public class SinkToMySql extends RichSinkFunction<Student> {

    private Connection connection;
    PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "insert into student(id, name, age) values (?, ?, ?)";
        assert connection != null;
        statement = connection.prepareStatement(sql);

        System.out.println("open----");
    }

    private Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://172.32.2.17:3306/0106_test?useSSL=false&serverTimezone=UTC", "root", "root12345678");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        statement.setInt(1, value.getId());
        statement.setString(2, value.getName());
        statement.setInt(3, value.getAge());
        statement.executeLargeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
        System.out.println("close");
    }
}
