package com.jbwang.flink.project

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.collection.mutable

/**
 * 自定义mysql的source
 */
class MySqlSource extends RichParallelSourceFunction[mutable.HashMap[String, String]] {

    var connection: Connection = null
    var ps: PreparedStatement = null

    override def open(parameters: Configuration): Unit = {
        Class.forName("com.mysql.jdbc.Driver")
        connection = DriverManager.getConnection("jdbc:mysql://172.32.2.17:3306/0106_test?useSSL=false&serverTimezone=UTC", "root", "root12345678")

        val sql = "select user_id, domain from user_domain_config"
        ps = connection.prepareStatement(sql)
    }

    override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {
        val result = ps.executeQuery()
        if (null != result) {
            val map = new mutable.HashMap[String, String]
            while (result.next()) {
                val userId = result.getString(1)
                val domain = result.getString(2)
                map.put(domain, userId)
            }
            ctx.collect(map)
        }
    }

    override def cancel(): Unit = ???

    override def close(): Unit = {
        if (ps != null) {
            ps.close()
        }
        if (connection != null) {
            connection.close()
        }
    }
}
