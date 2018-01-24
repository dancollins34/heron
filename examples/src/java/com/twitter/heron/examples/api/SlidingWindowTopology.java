//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.twitter.heron.examples.api;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.BaseWindowedBolt;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.PrinterBolt;
import com.twitter.heron.examples.api.bolt.SlidingWindowSumBolt;
import com.twitter.heron.examples.api.bolt.TumblingWindowAvgBolt;
import com.twitter.heron.examples.api.spout.RandomIntegerSpout;

/**
 * A sample topology that demonstrates the usage of {@link com.twitter.heron.api.bolt.IWindowedBolt}
 * to calculate sliding window sum.
 */
public final class SlidingWindowTopology implements AbstractExampleTopology{
  public static void main(String[] args) throws Exception {
    new SlidingWindowTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("integer", new RandomIntegerSpout(), 1);
    builder.setBolt("slidingsum", new SlidingWindowSumBolt()
        .withWindow(BaseWindowedBolt.Count.of(30), BaseWindowedBolt.Count.of(10)), 1)
        .shuffleGrouping("integer");
    builder.setBolt("tumblingavg", new TumblingWindowAvgBolt()
        .withTumblingWindow(BaseWindowedBolt.Count.of(3)), 1)
        .shuffleGrouping("slidingsum");
    builder.setBolt("printer", new PrinterBolt(), 1)
        .shuffleGrouping("tumblingavg");

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);

    conf.setComponentRam("integer", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("slidingsum", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("tumblingavg", ByteAmount.fromGigabytes(1));
    conf.setComponentRam("printer", ByteAmount.fromGigabytes(1));

    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(5));
    conf.setContainerCpuRequested(4);

    return conf;
  }
}
