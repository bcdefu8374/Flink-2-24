package com.clj.apitest.source;

import com.clj.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author chen
 * @topic
 * @create 2021-03-21
 */
public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //并行度设置为1，就可以达到输出结果是顺序的，此时也不再有并行度的序号。
        env.setParallelism(1);

        //2.从集合中读取数据
        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 37.5),
                new SensorReading("sensor_7", 1547718202L, 5.8),
                new SensorReading("sensor_10", 1547718205L, 50.9)
        ));

        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 5, 45, 89, 185);

        //打印输出
        dataStream.print("data");
        integerDataStream.print("int");


        //执行
        env.execute();

    }
}
