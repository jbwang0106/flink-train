package com.jbwang.flink.course06

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object TableSqlApi {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment
        val tableEnv = BatchTableEnvironment.create(env)

        // 读取csv
        val filePath = "file:///D:/BaiduNetdiskDownload/20-flink/input/hello.csv"
        val csv = env.readCsvFile[SalaryLog](filePath, ignoreFirstLine = true)

        // 注册一个table
//        val table = tableEnv.fromDataSet(csv)
//        tableEnv.registerTable("sales", table)
        tableEnv.registerDataSet("myTable", csv)

        // 进行查询
        val resultTable = tableEnv.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId")
        tableEnv.toDataSet[Row](resultTable).print()
    }

}

case class SalaryLog(transactionId: String,customerId: String,itemId: String,amountPaid: Double) {

}
