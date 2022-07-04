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

package com.ncst.training.ride.cleansing;

import com.ncst.training.common.datatypes.TaxiRide;
import com.ncst.training.common.sources.TaxiRideGenerator;
import com.ncst.training.common.utils.MissingSolutionException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Flink训练的出租车清理练习。
 * 这个练习的任务是过滤出租车出行记录的数据流，以只保留开始和结束都在纽约市的出行。
 * 产生的流应该被打印。
 */
public class RideCleansingExercise {

    private final SourceFunction<TaxiRide> source;
    private final SinkFunction<TaxiRide> sink;

    /** Creates a job using the source and sink provided. */
    public RideCleansingExercise(SourceFunction<TaxiRide> source, SinkFunction<TaxiRide> sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        RideCleansingExercise job =
                new RideCleansingExercise(new TaxiRideGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the pipeline
        env.addSource(source)
                .filter(new FilterFunction<TaxiRide>() {
                    @Override
                    public boolean filter(TaxiRide value) throws Exception {
                        return false;
                    }
                })
                .addSink(sink);

        // run the pipeline and return the result
        return env.execute("Taxi Ride Cleansing");
    }

    /** Keep only those rides and both start and end in NYC. */
    public static class NYCFilter implements FilterFunction<TaxiRide> {
        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            throw new MissingSolutionException();
        }
    }
}
