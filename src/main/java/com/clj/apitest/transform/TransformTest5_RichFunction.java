package com.clj.apitest.transform;

import com.clj.WordC.WordCount;
import com.clj.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2021-04-07
 */
public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //转化为SensorReading
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        //获取到运行时上下文
        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        env.execute();
    }

//    public static class MyMapper implements MapFunction<SensorReading,Tuple2<String,Integer>>{
//
//        @Override
//        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
//            return new Tuple2<>(value.getId(),value.getId().length());
//        }
//    }

    //实现自定义富函数类
    public static class MyMapper extends RichMapFunction<SensorReading,Tuple2<String,Integer>>{

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {

            //获取运行时上下文
            return new Tuple2<>(value.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，一般是定义状态，或者建立数据库连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            //一般是关闭连接和清空状态的收尾操作
            System.out.println("close");
        }
    }


}
