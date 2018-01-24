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

import java.util.ArrayList;
import java.util.List;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.grouping.CustomStreamGrouping;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.CountPrintBolt;
import com.twitter.heron.examples.api.spout.TestWordSpout;
/**
 * This is a basic example of a Storm topology.
 */
public final class CustomGroupingTopology implements AbstractExampleTopology{
  public static void main(String[] args) throws Exception {
    new CustomGroupingTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 2);
    builder.setBolt("mybolt", new CountPrintBolt(10000), 2)
        .customGrouping("word", new MyCustomStreamGrouping());

    return builder.createTopology();
  }

  @Override
  public Config buildConfig() {
    Config conf = new Config();

    // component resource configuration
    conf.setComponentRam("word", ByteAmount.fromMegabytes(512));
    conf.setComponentRam("mybolt", ByteAmount.fromMegabytes(512));

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerRamRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerCpuRequested(2);

    conf.setNumStmgrs(2);

    return conf;
  }

  public static class MyCustomStreamGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = 5987557161936201860L;
    private List<Integer> taskIds;

    public MyCustomStreamGrouping() {
    }

    @Override
    public void prepare(TopologyContext context,
                        String component, String streamId,
                        List<Integer> targetTasks) {
      this.taskIds = targetTasks;
    }

    @Override
    public List<Integer> chooseTasks(List<Object> values) {
      List<Integer> ret = new ArrayList<>();
      ret.add(taskIds.get(0));
      return ret;
    }
  }
}
