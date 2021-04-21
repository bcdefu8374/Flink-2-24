package com.clj.apitest.sink;

import com.clj.apitest.beans.SensorReading;
import com.clj.apitest.source.SourceTest4_UDF;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author chen
 * @topic
 * @create 2021-04-11
 */
public class SinkTest04_Jdbc {
    public static void main(String[] args) throws Exception{
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //从文件读取数据
//        DataStream<String> inputStream = env.readTextFile("F:\\bigdata\\IDEA\\items\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
//
//        //转化为SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(line -> {
//            String[] split = line.split(",");
//            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
//        });

        //定义一个
        DataStream<SensorReading> dataStream = env.addSource(new SourceTest4_UDF.MySensorSource());

        dataStream.addSink(new MyJdbcSink());

        env.execute();
    }

    //实现自定义的SinkFunction
    public static class MyJdbcSink extends RichSinkFunction<SensorReading> {

        //在外边进行一个声明，在里面进行赋值
        Connection connection = null;

        //声明预编译器，为提高效率，数据来了之后直接填写进去就可以了
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        //在open生命周期里面创建一个连接
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456");
            insertStmt = connection.prepareStatement("insert  into sensor_temp (id,temp) values (?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");

        }

        //每来一条数据，调用连接，执行sql
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            //直接执行更新语句，如果没有更新那么就插入。
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2,value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1,value.getId());
                insertStmt.setDouble(2,value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
