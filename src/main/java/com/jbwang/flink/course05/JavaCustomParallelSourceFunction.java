package com.jbwang.flink.course05;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author jbwang0106
 */
public class JavaCustomParallelSourceFunction implements ParallelSourceFunction<Long> {

    private long count = 1L;

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count ++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
