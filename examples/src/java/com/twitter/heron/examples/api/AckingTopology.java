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
import com.twitter.heron.examples.api.bolt.ExclamationBolt;
import com.twitter.heron.examples.api.spout.AckingTestWordSpout;

/**
 * This is a basic example of a Heron topology with acking enable.
 */
public final class AckingTopology implements AbstractExampleTopology{
  private static int spouts = 2;
  private static int bolts = 2;

  public static void main(String[] args) throws Exception {
    new AckingTopology().run(args);
  }


  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new AckingTestWordSpout(), spouts);
    builder.setBolt("exclaim1", new ExclamationBolt(true, 10000, false, true), bolts)
        .shuffleGrouping("word");

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);

    // Specifies that all tuples will be automatically failed if not acked within 10 seconds
    conf.setMessageTimeoutSecs(10);

    // Put an arbitrarily large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable at-least-once delivery semantics
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);

    // Extra JVM options
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // Component resource configuration
    conf.setComponentRam("word", ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim1", ExampleResources.getComponentRam());

    // Container resource configuration
    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(spouts + bolts, 2));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(spouts + bolts, 2));
    conf.setContainerCpuRequested(1);

    // Set the number of workers or stream managers
    conf.setNumStmgrs(2);

    return conf;
  }
}
