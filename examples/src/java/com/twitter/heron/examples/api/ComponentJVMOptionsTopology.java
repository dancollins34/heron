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
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.ExclamationBolt;
import com.twitter.heron.examples.api.spout.TestWordSpout;


/**
 * This is a basic example of a Storm topology.
 */
public final class ComponentJVMOptionsTopology implements AbstractExampleTopology{
  public static void main(String[] args) throws Exception {
    new ComponentJVMOptionsTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(false, 100000, false, false), 2)
        .shuffleGrouping("word")
        .addConfiguration("test-config", "test-key"); // Sample adding component-specific config

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);

    // TOPOLOGY_WORKER_CHILDOPTS will be a global one
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // For each component, both the global and if any the component one will be appended.
    // And the component one will take precedence
    conf.setComponentJvmOptions("word", "-XX:NewSize=300m");
    conf.setComponentJvmOptions("exclaim1", "-XX:NewSize=300m");

    // component resource configuration
    conf.setComponentRam("word", ByteAmount.fromMegabytes(512));
    conf.setComponentRam("exclaim1", ByteAmount.fromMegabytes(512));

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerRamRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerCpuRequested(2);

    // Specify the size of ram padding to per container.
    // Notice, this config will be considered as a hint,
    // and it's up to the packing algorithm to determine whether to apply this hint
    conf.setContainerRamPadding(ByteAmount.fromGigabytes(2));
    conf.setNumStmgrs(2);

    return conf;
  }
}
