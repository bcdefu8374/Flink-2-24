package com.clj.apitest.sink;

import com.clj.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author chen
 * @topic
 * @create 2021-04-07
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转化为SensorReading
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2])).toString();
        });

        //输出到kafka
        dataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092","sinktest",new SimpleStringSchema()));


        //执行
        env.execute();
    }
}
