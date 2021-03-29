package com.clj.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author chen
 * @topic
 * @create 2021-03-22
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建properties,kafka连接
        Properties properties = new Properties();
        properties.getProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id", "test");



        //2.读取数据
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));

        //3.打印
        dataStream.print();


        //4.执行
        env.execute();

    }
}
