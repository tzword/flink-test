package com.example.flink.sql;

import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class CompanyExec {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
            // 设置执行环境
        Configuration config = new Configuration();
        config.setInteger("rest.port", 8052);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
            // 配置MySQL-CDC Source
        TableResult stdSource = tEnv.executeSql(
            "CREATE TABLE company_std (\n" +
                "  csf_id STRING NOT NULL,\n" +
                "  std_name STRING,\n" +
                "  PRIMARY KEY (csf_id) NOT ENFORCED\n" +
                ") WITH (\n" +
            "  'connector' = 'mysql-cdc',\n" +
            "  'hostname' = 'localhost',\n" +
            "  'port' = '3306',\n" +
            "  'username' = 'root',\n" +
            "  'password' = '123456',\n" +
            "  'database-name' = 'company',\n" +
            "  'table-name' = 'company_std',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n"+
            ")");

        tEnv.executeSql(
            "CREATE TABLE company_std_temp (\n" +
                "  csf_id STRING NOT NULL,\n" +
                "  std_name STRING,\n" +
                "  PRIMARY KEY (csf_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://localhost:3306/company',\n" +
                "  'table-name' = 'company_std',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456'\n" +
                ")"
        );

        TableResult samProduct = tEnv.executeSql("CREATE TABLE company_sam_product (\n" +
            "  id BIGINT,\n" +
            "  csf_id STRING,\n" +
            "  product_code STRING,\n" +
            "  PRIMARY KEY (id) NOT ENFORCED\n" +
            ") WITH (\n" +
            "  'connector' = 'mysql-cdc',\n" +
            "  'hostname' = 'localhost',\n" +
            "  'port' = '3306',\n" +
            "  'username' = 'root',\n" +
            "  'password' = '123456',\n" +
            "  'database-name' = 'company',\n" +
            "  'table-name' = 'company_sam_product',\n" +
            "  'scan.startup.mode' = 'earliest-offset'\n"+
            ")");
        tEnv.executeSql("CREATE TABLE company_sql_sink (\n" +
            "  csfId STRING,\n" +
            "  stdName STRING,\n" +
            "  productCodes ARRAY<STRING>,\n" +
            " PRIMARY KEY (csfId) NOT ENFORCED\n"+
            ") WITH (\n" +
            "  'connector' = 'elasticsearch-7',\n" +
            "  'hosts' = 'http://localhost:9200',\n" +
            "  'index' = 'company_sql_sink',\n" +
            "    'format' = 'json' \n" +
            ");");

        tEnv.createTemporarySystemFunction("parseArrayDemo", MultisetToString.class);
//        统一更新方案
//        TableResult tableResult = tEnv.executeSql(
//            "insert into company_sql_sink " +
//                "SELECT \n" +
//            "    c.csf_id AS csfId,\n" +
//            "    c.std_name AS stdName,\n" +
//            "    parseArrayDemo(collect(s.product_code)) AS productCodes\n" +
//            "FROM company_std AS c\n" +
//            "LEFT JOIN company_sam_product AS s ON c.csf_id = s.csf_id\n" +
//            "GROUP BY c.csf_id, c.std_name;");
//        tableResult.print();
//        System.out.println(tableResult.getJobClient().get().getJobStatus());

        //单个更新方案
        TableResult tableResult = tEnv.executeSql(
            "insert into company_sql_sink(csfId,stdName) " +
                "SELECT \n" +
                "    c.csf_id AS csfId,\n" +
                "    c.std_name AS stdName \n" +
                "FROM company_std AS c;\n");
        tableResult.print();
        TableResult tableResult1 = tEnv.executeSql(
            "insert into company_sql_sink(csfId,productCodes) " +
                "SELECT \n" +
                "    s.csf_id AS csfId,\n" +
                "    parseArrayDemo(collect(s.product_code)) AS productCodes\n" +
                "FROM company_sam_product s group by s.csf_id" +
                ";\n");


        tableResult1.print();



        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
