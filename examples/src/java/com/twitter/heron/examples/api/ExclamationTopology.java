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

import java.time.Duration;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.examples.api.bolt.UpdatableExclamationBolt;
import com.twitter.heron.examples.api.spout.TestWordSpout;

/**
 * This is a basic example of a Storm topology.
 */
public final class ExclamationTopology implements AbstractExampleTopology{
  static int parallelism = 2;
  static int spouts = parallelism;
  static int bolts = 2*parallelism;

  public static void main(String[] args) throws Exception {
    new ExclamationTopology().run(args);
  }


  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(Duration.ofMillis(0)), spouts);
    builder.setBolt("exclaim1", new UpdatableExclamationBolt(false, 10000), bolts)
        .shuffleGrouping("word");

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);
    conf.setMaxSpoutPending(10);
    conf.setMessageTimeoutSecs(600);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // resources configuration
    conf.setComponentRam("word", ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim1",
        ExampleResources.getComponentRam());

    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(spouts + bolts, parallelism));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(spouts + bolts, parallelism));
    conf.setContainerCpuRequested(1);

    conf.setNumStmgrs(parallelism);

    return conf;
  }
}
