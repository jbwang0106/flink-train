package com.jbwang.flink.course06;

import lombok.Data;
import lombok.ToString;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jbwang0106
 */
public class JavaTableSqlApi {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(environment);

        DataSource<Sale> csv = environment.readCsvFile("file:///D:/BaiduNetdiskDownload/20-flink/input/hello.csv")
                .ignoreFirstLine()
                .pojoType(Sale.class, "transactionId", "customerId", "item", "amountPaid");

        tableEnvironment.registerDataSet("sales", csv);
        Table table = tableEnvironment.sqlQuery("select customerId, sum(amountPaid) money from sales group by customerId");

        tableEnvironment.toDataSet(table, Row.class).print();

    }

    @Data
    @ToString
    public static class Sale {

        private String transactionId;

        private String customerId;

        private String item;

        private Double amountPaid;
    }
}
