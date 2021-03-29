package com.clj.apitest.source;

import com.clj.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author chen
 * @topic
 * @create 2021-03-22
 */
public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //全局并行度设置为1
        env.setParallelism(1);

        //2.从文件读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        //3.打印输出
        dataStream.print();

        //4.执行
        env.execute();

    }

    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading>{

        //定义标志位用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {

            //定义一个随机数发生器
            Random random = new Random();

            //设置10个传感器的初始温度
            HashMap<String,Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i+1), 60 + random.nextGaussian() * 20);

            }

            while ( running = true){
                for ( String sensorId: sensorTempMap.keySet()){
                    //在当前温度基础上随机波动
                    Double newtemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId,newtemp);
                    ctx.collect(new SensorReading(sensorId,System.currentTimeMillis(),newtemp));

                }

                //控制输出频率
                Thread.sleep(1000L);

            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
