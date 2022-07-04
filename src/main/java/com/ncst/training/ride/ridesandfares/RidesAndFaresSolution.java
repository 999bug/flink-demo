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

package com.ncst.training.ride.ridesandfares;

import com.ncst.training.common.datatypes.RideAndFare;
import com.ncst.training.common.datatypes.TaxiFare;
import com.ncst.training.common.datatypes.TaxiRide;
import com.ncst.training.common.sources.TaxiFareGenerator;
import com.ncst.training.common.sources.TaxiRideGenerator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the Stateful Enrichment exercise from the Flink training.
 *
 * <p>The goal for this exercise is to enrich TaxiRides with fare information.
 */
public class RidesAndFaresSolution {

    private final SourceFunction<TaxiRide> rideSource;
    private final SourceFunction<TaxiFare> fareSource;
    private final SinkFunction<RideAndFare> sink;

    /**
     * Creates a job using the sources and sink provided.
     */
    public RidesAndFaresSolution(
            SourceFunction<TaxiRide> rideSource,
            SourceFunction<TaxiFare> fareSource,
            SinkFunction<RideAndFare> sink) {

        this.rideSource = rideSource;
        this.fareSource = fareSource;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        RidesAndFaresSolution job =
                new RidesAndFaresSolution(
                        new TaxiRideGenerator(),
                        new TaxiFareGenerator(),
                        new PrintSinkFunction<>());

        // Setting up checkpointing so that the state can be explored with the State Processor API.
        // Generally it's better to separate configuration settings from the code,
        // but for this example it's convenient to have it here for running in the IDE.
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.checkpoints.dir", "file:///D:/code/flink/flink-training/checkpoints");
        conf.setString("execution.checkpointing.interval", "10s");
        conf.setString(
                "execution.checkpointing.externalized-checkpoint-retention",
                "RETAIN_ON_CANCELLATION");

        // 启动 web ui
        conf.setString(RestOptions.BIND_PORT, "10086");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        job.execute(env);
    }

    /**
     * Creates and executes the pipeline using the StreamExecutionEnvironment provided.
     *
     * @param env The {StreamExecutionEnvironment}.
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute(StreamExecutionEnvironment env) throws Exception {

        // A stream of taxi ride START events, keyed by rideId.
        DataStream<TaxiRide> rides = env.addSource(rideSource)
                .name("车费")
                .filter(ride -> ride.isStart)
                // 对流进行分区
                .keyBy(ride -> ride.rideId);

        // A stream of taxi fare events, also keyed by rideId.
        DataStream<TaxiFare> fares = env.addSource(fareSource).name("车程")
                .keyBy(fare -> fare.rideId);

        // Create the pipeline.
        rides.connect(fares)
                .flatMap(new EnrichmentFunction())
                .uid("enrichment") // uid for this operator's state
                .name("enrichment11") // name for this operator in the web UI
                .addSink(sink);

        // Execute the pipeline and return the result.
        return env.execute("Join Rides with Fares");
    }

    /**
     * Creates and executes the pipeline using the default StreamExecutionEnvironment.
     */
    public JobExecutionResult execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        return execute(env);
    }

    public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, RideAndFare> {
        /**
         * 一个算子(operator)中的 ValueState keyed state 作用域默认是属于它所属的key的。
         * 状态在使用之前需要先被注册，即实现 open() 方法，-->初始化
         */
        private ValueState<TaxiRide> rideState;
        private ValueState<TaxiFare> fareState;

        @Override
        public void open(Configuration config) {

            rideState = getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
            fareState = getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
        }

        @Override
        public void flatMap1(TaxiRide ride, Collector<RideAndFare> out) throws Exception {
            TaxiFare fare = fareState.value();
            if (fare != null) {
                fareState.clear();
                out.collect(new RideAndFare(ride, fare));
            } else {
                rideState.update(ride);
            }
        }

        @Override
        public void flatMap2(TaxiFare fare, Collector<RideAndFare> out) throws Exception {
            TaxiRide ride = rideState.value();
            if (ride != null) {
                rideState.clear();
                out.collect(new RideAndFare(ride, fare));
            } else {
                fareState.update(fare);
            }
        }
    }
}