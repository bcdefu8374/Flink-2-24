package com.clj.apitest.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2021-04-07
 */
public class TranstormTest6_Partition {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //读取文件
        DataStreamSource<String> dataStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\hello.txt");

        //打印
        dataStream.print("input");

        //1.shuffle
        DataStream<String> shuffleStream = dataStream.shuffle();
        shuffleStream.print("shuffle");



        //执行
        env.execute();


    }
}
