package com.jbwang.flink.course04

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetDataTransformationApp {

    def main(args: Array[String]): Unit = {

        val env = ExecutionEnvironment.getExecutionEnvironment

        // mapFunction(env)
       // filterFunction(env)
       // mapPartitionFunction(env)

       // firstFunction(env)
       // flatMapFunction(env)
      //  distinctFunction(env)

       // joinFunction(env)
       // outerJoinFunction(env)
        crossFunction(env)
    }

    // 笛卡尔积
    def crossFunction(env: ExecutionEnvironment): Unit = {
        val info1 = List("曼联", "曼城")
        val info2 = List(3, 1, 0)

        val data1 = env.fromCollection(info1)
        val data2 = env.fromCollection(info2)

        data1.cross(data2).print()
    }

    def outerJoinFunction(env: ExecutionEnvironment): Unit = {
        val info1 = ListBuffer[(Int, String)]()
        info1.append((1, "Flink"))
        info1.append((2, "Spark"))
        info1.append((3, "Hadoop"))
        info1.append((4, "Storm"))

        val info2 = ListBuffer[(Int, String)]()
        info2.append((1, "Ali"))
        info2.append((2, "Apache"))
        info2.append((5, "Not"))
        info2.append((4, "Yahu"))

        val data1 = env.fromCollection(info1)
        val data2 = env.fromCollection(info2)

//        data1.leftOuterJoin(data2)
//            .where(0).equalTo(0)
//            .apply((first, second) => {
//                if (second == null) {
//                    (first._1, first._2, "-")
//                } else {
//                    (first._1, first._2, second._2)
//                }
//            }).print()

//        data1.rightOuterJoin(data2)
//            .where(0).equalTo(0)
//            .apply((first, second) => {
//                if (first == null) {
//                    (second._1, "-", second._2)
//                } else {
//                    (first._1, first._2, second._2)
//                }
//            }).print()

        data1.fullOuterJoin(data2)
            .where(0).equalTo(0)
            .apply((first, second) => {
                if (first == null) {
                    (second._1, "-", second._2)
                } else if (second == null) {
                    (first._1, first._2, "-")
                } else {
                    (first._1, first._2, second._2)
                }
            }).print()

    }

    def joinFunction(env: ExecutionEnvironment): Unit = {
        val info1 = ListBuffer[(Int, String)]()
        info1.append((1, "Flink"))
        info1.append((2, "Spark"))
        info1.append((3, "Hadoop"))
        info1.append((4, "Storm"))

        val info2 = ListBuffer[(Int, String)]()
        info2.append((1, "Ali"))
        info2.append((2, "Apache"))
        info2.append((5, "Not"))
        info2.append((4, "Yahu"))

        val data1 = env.fromCollection(info1)
        val data2 = env.fromCollection(info2)

        data1.join(data2)
            .where(0).equalTo(0)
            .apply((first, second) => {
                (first._1, first._2, second._2)
            }).print()

    }

    def distinctFunction(env: ExecutionEnvironment): Unit = {
        val info = ListBuffer[String]()

        info.append("hadoop,spark")
        info.append("spark,flink")
        info.append("hadoop,flink")

        val data = env.fromCollection(info)

        data.flatMap(_.split(",")).distinct().print()
    }

    def flatMapFunction(env: ExecutionEnvironment): Unit = {
        val info = ListBuffer[String]()

        info.append("hadoop,spark")
        info.append("spark,flink")
        info.append("hadoop,flink")

        val data = env.fromCollection(info)

        data.flatMap(_.split(","))
            .map((_, 1))
            .groupBy(0)
            .sum(1)
            .print()
    }

    // first -n
    def firstFunction(env: ExecutionEnvironment): Unit = {
        val info = ListBuffer[(Int, String)]()
        info.append((1, "Flink"))
        info.append((1, "Spark"))
        info.append((1, "Hadoop"))
        info.append((2, "Java"))
        info.append((2, "Scala"))
        info.append((3, "Linux"))
        info.append((4, "Vue-js"))

        val data = env.fromCollection(info)

        // 取前三
       // data.first(3).print()

        // 分组后取每组的前两个
        //data.groupBy(0).first(2).print()

        // 分组排序之后取每组的前2
        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()

    }

    // map partition function 将map按照并行度分成对应的分区，每个分区只会获取一个数据库链接之类的数据
    def mapPartitionFunction(environment: ExecutionEnvironment): Unit = {
        val students = ListBuffer[String]()
        for (i <- 1 to 100) {
            students.append("student: " + i)
        }

        val data = environment.fromCollection(students).setParallelism(6)

        data.mapPartition(x => {
            val connection = DBUtils.getConnection()
            println(connection + "....")
            DBUtils.returnConnection(connection)
            x
        }).print()

    }

    // map function
    def mapFunction(environment: ExecutionEnvironment): Unit = {

        environment.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            // .map(_ * 2) // (x:  Int) => x + 1
            .map((x:  Int) => x + 1)
            .map((x) => x + 1)
            .map(x => x + 1)
            .map(_ * 2)
            .print()

    }

    // filter function
    def filterFunction(environment: ExecutionEnvironment): Unit = {

        environment.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
            .map(_ + 2)
            .filter(_ % 2 == 0)
            .print()

    }

}

