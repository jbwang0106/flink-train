package com.jbwang.flink.course05

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * scala的自定义source
 */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {

    var count = 1
    var isRunning = true

    override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
        while (isRunning) {
            sourceContext.collect(count)
            count += 1
            Thread.sleep(1000)
        }
    }

    override def cancel(): Unit = isRunning = false
}
