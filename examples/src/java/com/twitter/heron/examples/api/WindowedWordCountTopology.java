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
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.examples.api.bolt.SplitSentenceBolt;
import com.twitter.heron.examples.api.bolt.WindowSumBolt;
import com.twitter.heron.examples.api.spout.StaticSentenceSpout;

public final class WindowedWordCountTopology implements AbstractExampleTopology{
  static int parallelism = 1;

  public static void main(String[] args) throws Exception {
    new WindowedWordCountTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("sentence", new StaticSentenceSpout(), parallelism);
    builder.setBolt("split", new SplitSentenceBolt(), parallelism).shuffleGrouping("sentence");
    builder.setBolt("consumer", new WindowSumBolt()
        .withWindow(BaseWindowedBolt.Count.of(10)), parallelism)
        .fieldsGrouping("split", new Fields("word"));

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    return new Config();
  }
}
