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
public final class MultiSpoutExclamationTopology implements AbstractExampleTopology{
  public static void main(String[] args) throws Exception {
    new MultiSpoutExclamationTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word0", new TestWordSpout(), 2);
    builder.setSpout("word1", new TestWordSpout(), 2);
    builder.setSpout("word2", new TestWordSpout(), 2);
    builder.setBolt("exclaim1", new ExclamationBolt(false, 100000, false, false), 2)
        .shuffleGrouping("word0")
        .shuffleGrouping("word1")
        .shuffleGrouping("word2");

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    conf.setComponentRam("word0", ExampleResources.getComponentRam());
    conf.setComponentRam("word1", ExampleResources.getComponentRam());
    conf.setComponentRam("word2", ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim1", ExampleResources.getComponentRam());

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3));
    conf.setContainerRamRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerCpuRequested(1);

    conf.setNumStmgrs(3);

    return conf;
  }
}
