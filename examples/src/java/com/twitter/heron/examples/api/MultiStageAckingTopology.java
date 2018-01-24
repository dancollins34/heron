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

import java.util.Map;
import java.util.Random;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.examples.api.bolt.ExclamationBolt;
import com.twitter.heron.examples.api.spout.AckingTestWordSpout;
import com.twitter.heron.simulator.Simulator;

/**
 * This is three stage topology. Spout emits to bolt to bolt.
 */
public final class MultiStageAckingTopology implements AbstractExampleTopology{
  static int parallelism = 2;

  public static void main(String[] args) throws Exception {
    new MultiSpoutExclamationTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new AckingTestWordSpout(1), parallelism);
    builder.setBolt("exclaim1", new ExclamationBolt(true, 10000, true, false), parallelism)
        .shuffleGrouping("word");
    builder.setBolt("exclaim2", new ExclamationBolt(true, 10000, false, false), parallelism)
        .shuffleGrouping("exclaim1");

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking, we need to setEnableAcking true
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);

    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    conf.setComponentRam("word",
        ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim1",
        ExampleResources.getComponentRam());
    conf.setComponentRam("exclaim2",
        ExampleResources.getComponentRam());

    // container resource configuration
    conf.setContainerDiskRequested(
        ExampleResources.getContainerDisk(3 * parallelism, parallelism));
    conf.setContainerRamRequested(
        ExampleResources.getContainerRam(3 * parallelism, parallelism));
    conf.setContainerCpuRequested(1);

    conf.setNumStmgrs(parallelism);

    return conf;
  }
}
