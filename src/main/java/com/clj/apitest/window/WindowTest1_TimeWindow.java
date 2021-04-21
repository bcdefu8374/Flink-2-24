package com.clj.apitest.window;

import com.clj.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author chen
 * @topic
 * @create 2021-04-20
 */
public class WindowTest1_TimeWindow {
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

        //开窗测试
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id")
//                .countWindow(10,2)  //传两个参数就表示计数滑动窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .timeWindow(Time.seconds(15))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

//        //2.全窗口函数
//        SingleOutputStreamOperator<Integer> resultStream2 = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(15))
//                .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
//
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Integer> out) throws Exception {
//                        Integer count = IteratorUtils.toList(input.iterator()).size();
//                        out.collect(count);
//                    }
//                });

        //2.全窗口函数
        SingleOutputStreamOperator<Tuple3<String,Long,Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id,windowEnd,count));
                    }
                });


        //3.其他可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {};
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .trigger(new Trigger<SensorReading, TimeWindow>() {
//                    @Override
//                    public TriggerResult onElement(SensorReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return null;
//                    }
//
//                    @Override
//                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//
//                    }
//                })
//               .evictor(new Evictor<SensorReading, TimeWindow>() {
//                    @Override
//                    public void evictBefore(Iterable<TimestampedValue<SensorReading>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
//
//                    }
//
//                    @Override
//                    public void evictAfter(Iterable<TimestampedValue<SensorReading>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
//
//                    }
//                })
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        //打印
        sumStream.getSideOutput(outputTag).print("late");


        resultStream.print();
        resultStream2.print();

        env.execute();
    }
}
