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
import com.twitter.heron.examples.api.bolt.SplitSentenceBolt;
import com.twitter.heron.examples.api.bolt.WordCountBolt;
import com.twitter.heron.examples.api.spout.FastRandomSentenceSpout;

public final class SentenceWordCountTopology implements AbstractExampleTopology{
  public static void main(String[] args) throws Exception {
    new SentenceWordCountTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new FastRandomSentenceSpout(), 1);
    builder.setBolt("split", new SplitSentenceBolt(), 2).shuffleGrouping("spout");
    builder.setBolt("count", new WordCountBolt(), 2).fieldsGrouping("split", new Fields("word"));

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();

    // component resource configuration
    conf.setComponentRam("spout", ByteAmount.fromMegabytes(512));
    conf.setComponentRam("split", ByteAmount.fromMegabytes(512));
    conf.setComponentRam("count", ByteAmount.fromMegabytes(512));

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(3));
    conf.setContainerRamRequested(ByteAmount.fromGigabytes(3));
    conf.setContainerCpuRequested(2);

    conf.setNumStmgrs(2);

    return conf;
  }
}
