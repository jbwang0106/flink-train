package com.jbwang.flink.course04

import scala.util.Random

object DBUtils {

    def getConnection(): String = {
        new Random(10).nextInt() + ""
    }

    def returnConnection(connection: String): Unit = {

    }

}
