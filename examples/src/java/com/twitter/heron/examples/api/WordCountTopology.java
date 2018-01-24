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
import com.twitter.heron.examples.api.bolt.WordCountBolt;
import com.twitter.heron.examples.api.spout.RandomWordSpout;

/**
 * This is a topology that does simple word counts.
 * <p>
 * In this topology,
 * 1. the spout task generate a set of random words during initial "open" method.
 * (~128k words, 20 chars per word)
 * 2. During every "nextTuple" call, each spout simply picks a word at random and emits it
 * 3. Spouts use a fields grouping for their output, and each spout could send tuples to
 * every other bolt in the topology
 * 4. Bolts maintain an in-memory map, which is keyed by the word emitted by the spouts,
 * and updates the count when it receives a tuple.
 */
public final class WordCountTopology implements AbstractExampleTopology{
  static int parallelism = 1;

  /**
   * Main method
   */
  public static void main(String[] args) throws Exception {
    new WordCountTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new RandomWordSpout(), parallelism);
    builder.setBolt("consumer", new WordCountBolt(), parallelism)
        .fieldsGrouping("word", new Fields("word"));

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();
    conf.setNumStmgrs(parallelism);

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
