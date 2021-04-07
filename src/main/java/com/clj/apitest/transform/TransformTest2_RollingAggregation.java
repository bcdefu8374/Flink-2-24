package com.clj.apitest.transform;

import com.clj.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chen
 * @topic
 * @create 2021-03-22
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        //从文件读取数据
        DataStreamSource<String> inputStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

//        //转化为SensorReading类型
//       DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//           @Override
//           public SensorReading map(String value) throws Exception {
//               String[] fields  = value.split(",");
//
//               return new SensorReading(fields[0], new Long(fields[1]),new Double(fields[2]));
//           }
//       });



        //使用Lamdal表达式
        DataStream<SensorReading> dataStream = inputStream.map( line ->{
            //对数据进行切分
            String[] fields = line.split(",");
            //输出数据
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });


        //分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        //另一种方法
        //dataSream.keyBy(data -> data.getId());
        //或者
        //dataSream.keyBy(SensorReading::getId);


        //再滚动聚合，取当前最大值
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");

        //打印输出
        resultStream.print();


        //2.执行
        env.execute();
    }
    
}
