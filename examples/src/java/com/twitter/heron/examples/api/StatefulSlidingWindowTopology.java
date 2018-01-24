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
import com.twitter.heron.examples.api.bolt.StatefulWindowSumBolt;
import com.twitter.heron.examples.api.spout.StatefulRandomIntegerSpout;

/**
 * A sample topology that demonstrates the usage of {@link com.twitter.heron.api.bolt.IStatefulWindowedBolt}
 * to calculate sliding window sum.  Topology also demonstrates how stateful window processing is done
 * in conjunction with effectively once guarantees
 */
public final class StatefulSlidingWindowTopology implements AbstractExampleTopology {
  public static void main(String[] args) throws Exception {
    new StatefulSlidingWindowTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("integer", new StatefulRandomIntegerSpout(), 1);
    builder.setBolt("sumbolt", new StatefulWindowSumBolt().withWindow(BaseWindowedBolt.Count.of(5),
        BaseWindowedBolt.Count.of(3)), 1).shuffleGrouping("integer");
    builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("sumbolt");

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);
    String topoName = "test";

    Config.setComponentRam(conf, "integer", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "sumbolt", ByteAmount.fromGigabytes(1));
    Config.setComponentRam(conf, "printer", ByteAmount.fromGigabytes(1));

    Config.setContainerDiskRequested(conf, ByteAmount.fromGigabytes(5));
    Config.setContainerCpuRequested(conf, 4);

    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(20);
    conf.setMaxSpoutPending(1000);

    return conf;
  }
}
