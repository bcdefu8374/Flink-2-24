package com.clj.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2021-03-22
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //全局并行度设置为1
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> dataStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //3.打印输出
        dataStream.print();

        //4.执行
        env.execute();

    }
}
