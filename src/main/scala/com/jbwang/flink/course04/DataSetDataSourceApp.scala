package com.jbwang.flink.course04

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object DataSetDataSourceApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        // import org.apache.flink.api.scala._
        // from collection
        // env.fromCollection(1 to 10).print()

        // from file
      //  textFile(env)

        // from csv ...
       // csvFile(env)

      //  readRecursiveFiles(env)

        compressionFiles(env)
    }

    // 读取压缩文件
    def compressionFiles(environment: ExecutionEnvironment): Unit = {
        val  filePath = "file:///D:/BaiduNetdiskDownload/20-flink/compression"
        environment.readTextFile(filePath).print()
    }

    // 递归文件夹读取文件
    def readRecursiveFiles(environment: ExecutionEnvironment): Unit = {
        val filePath = "file:///D:/BaiduNetdiskDownload/20-flink/inputjoin"
        environment.readTextFile(filePath).print()
        println("~~~~~~~~~分割线~~~~~~~")
        val config = new Configuration
        config.setBoolean("recursive.file.enumeration", true)
        environment.readTextFile(filePath)
            .withParameters(config)
            .print()
    }

    def textFile(environment: ExecutionEnvironment): Unit = {
        val filePath = "file:///D:/BaiduNetdiskDownload/20-flink/inputformat"
        environment.readTextFile(filePath).print()
    }

    def csvFile(environment: ExecutionEnvironment): Unit = {
        import org.apache.flink.api.scala._
        val filePath = "file:///D:/BaiduNetdiskDownload/20-flink/input/hello.csv"
       // environment.readCsvFile[(String, Int, String)](filePath, ignoreFirstLine = true).print()
      //  environment.readCsvFile[(String, Int)](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
      //  environment.readCsvFile[MyClass](filePath, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
        environment.readCsvFile[Person](filePath, ignoreFirstLine = true, pojoFields = Array("name", "age", "work")).print()
    }

}

case class MyClass(name: String, age: Int)
