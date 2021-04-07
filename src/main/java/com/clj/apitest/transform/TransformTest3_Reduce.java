package com.clj.apitest.transform;

import com.clj.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2021-04-01
 */
public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<SensorReading> map = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0],new Long(split[1]),new Double(split[2]));
        });


//        //转化为SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map( line ->{
//            //对数据进行切分
//            String[] fields = line.split(",");
//            //输出数据
//            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//        });
//
//
//        //分组
//        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
//
//        //reduce聚合，取最大的的温度值，以及当前最新的时间戳
//        SingleOutputStreamOperator<SensorReading> resultstream = keyedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                return new SensorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
//            }
//        });
//
//        //也可以使用这样
////        SingleOutputStreamOperator<SensorReading> resultstream = keyedStream.reduce((curState, newData) -> {
////            return new SensorReading(curState.getId(), newData.getTimestamp(), Math.max(curState.getTemperature(), newData.getTemperature()));
////        });
//
//        //打印
//        resultstream.print();

        //执行
        env.execute();


    }

}
