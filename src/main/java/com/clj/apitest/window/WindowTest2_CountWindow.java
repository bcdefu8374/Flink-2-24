package com.clj.apitest.window;

import com.clj.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2021-04-20
 */
public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //从文件读取数据
//        DataStreamSource<String> inputStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        //换成socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        //转化为SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        //开计数窗口测试
        SingleOutputStreamOperator<Double> avgTempResultStream = dataStream.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double,Integer>,Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getTemperature(),accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0/accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f1,a.f1 + b.f0);
        }
    }
}
