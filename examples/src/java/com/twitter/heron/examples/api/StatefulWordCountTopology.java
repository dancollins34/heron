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
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.StatefulWordCountBolt;
import com.twitter.heron.examples.api.spout.RandomWordSpout;

public final class StatefulWordCountTopology implements AbstractExampleTopology{
  static int parallelism = 1;

  /**
   * Main method
   */
  public static void main(String[] args) throws Exception {
    new StatefulSlidingWindowTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new RandomWordSpout(), parallelism);
    builder.setBolt("consumer", new StatefulWordCountBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(20);

    // configure component resources
    conf.setComponentRam("word",
        ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB * 2));
    conf.setComponentRam("consumer",
        ByteAmount.fromMegabytes(ExampleResources.COMPONENT_RAM_MB * 2));

    // configure container resources
    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(2 * parallelism, parallelism));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(2 * parallelism, parallelism));
    conf.setContainerCpuRequested(2);

    return conf;
  }
}
