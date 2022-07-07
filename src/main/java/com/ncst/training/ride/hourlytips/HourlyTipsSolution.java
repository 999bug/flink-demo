/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ncst.training.ride.hourlytips;

import com.ncst.training.common.datatypes.TaxiFare;
import com.ncst.training.common.sources.TaxiFareGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * “每小时小费” 练习的任务是确定每小时赚取最多小费的司机。
 * 最简单的方法是通过两个步骤来解决这个问题：
 * 首先使用一个小时长的窗口来计算每个司机在一小时内的总小费，
 * 然后从该窗口结果流中找到每小时总小费最多的司机。
 */
public class HourlyTipsSolution {

    private final SourceFunction<TaxiFare> source;
    /**
     * key1:每小时结束时间戳，key2:该小时内获得消费最多的driverID, key3:消费总金额
     */
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /**
     * Creates a job using the source and sink provided.
     */
    public HourlyTipsSolution(SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        HourlyTipsSolution job = new HourlyTipsSolution(new TaxiFareGenerator(), new PrintSinkFunction<>());
        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {
        // set up streaming execution environment
        Configuration configuration = new Configuration();
        configuration.setString(RestOptions.BIND_PORT, "10086");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // start the data generator and arrange for watermarking
        DataStream<TaxiFare> fares = env.addSource(source)
                .name("taxiFare soure")
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order
                        WatermarkStrategy.<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((fare, t) -> fare.getEventTimeMillis()));

        // compute tips per hour for each driver
        DataStream<Tuple3<Long, Long, Float>> hourlyTips =
                fares.keyBy((TaxiFare fare) -> fare.driverId)
                        .window(TumblingEventTimeWindows.of(Time.hours(1)))
                        // 匿名内部类方式
//                        .process(new ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow>() {
//                            @Override
//                            public void process(Long aLong, Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) {
//                                float sumOfTips = 0F;
//                                for (TaxiFare f : elements) {
//                                    sumOfTips += f.tip;
//                                }
//                                out.collect(Tuple3.of(context.window().getEnd(), aLong, sumOfTips));
//                            }
//                        })
                        // 内部类方式
                         .process(new AddTips())
                        .name("compute after");

        // find the driver with the highest sum of tips for each hour
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .maxBy(2)
                .name("highest driver");
        hourlyMax.addSink(sink);

        /* You should explore how this alternative (commented out below) behaves.
         * In what ways is the same as, and different from, the solution above (using a windowAll)?
         */

        // 下面的结果会产生相同数据
//        DataStream<Tuple3<Long, Long, Float>> hourlyMax1 = hourlyTips
//                .keyBy(t -> t.f0)
//                .maxBy(2);
//
//        hourlyMax1.addSink(sink);

        // execute the transformation pipeline
        return env.execute("Hourly Tips");
    }

    /*
     * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
     */
    public static class AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        @Override
        public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) {
            float sumOfTips = 0F;
            for (TaxiFare f : fares) {
                sumOfTips += f.tip;
            }
            out.collect(Tuple3.of(context.window().getEnd(), key, sumOfTips));
        }
    }
}
