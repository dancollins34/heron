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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.hooks.ITaskHook;
import com.twitter.heron.api.hooks.info.BoltAckInfo;
import com.twitter.heron.api.hooks.info.BoltExecuteInfo;
import com.twitter.heron.api.hooks.info.BoltFailInfo;
import com.twitter.heron.api.hooks.info.EmitInfo;
import com.twitter.heron.api.hooks.info.SpoutAckInfo;
import com.twitter.heron.api.hooks.info.SpoutFailInfo;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.examples.api.bolt.ExclamationBolt;
import com.twitter.heron.examples.api.spout.AckingTestWordSpout;


public final class TaskHookTopology implements AbstractExampleTopology{
  public static void main(String[] args) throws Exception {
    new TaskHookTopology().run(args);
  }

  @Override
  public HeronTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new AckingTestWordSpout(), 2);
    builder.setBolt("count", new ExclamationBolt(true, 10000, false, true), 2)
        .shuffleGrouping("word");

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

    // Set the task hook
    List<String> taskHooks = new LinkedList<>();
    taskHooks.add("com.twitter.heron.examples.TaskHookTopology$TestTaskHook");
    conf.setAutoTaskHooks(taskHooks);

    // component resource configuration
    conf.setComponentRam("word", ByteAmount.fromMegabytes(512));
    conf.setComponentRam("count", ByteAmount.fromMegabytes(512));

    // container resource configuration
    conf.setContainerDiskRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerRamRequested(ByteAmount.fromGigabytes(2));
    conf.setContainerCpuRequested(2);


    conf.setNumStmgrs(2);

    return conf;
  }

  public static class TestTaskHook implements ITaskHook {
    private static final long N = 10000;
    private long emitted = 0;
    private long spoutAcked = 0;
    private long spoutFailed = 0;
    private long boltExecuted = 0;
    private long boltAcked = 0;
    private long boltFailed = 0;

    private String constructString;

    public TestTaskHook() {
      this.constructString = "This TestTaskHook is constructed with no parameters";
    }

    public TestTaskHook(String constructString) {
      this.constructString = constructString;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context) {
      GlobalMetrics.incr("hook_prepare");
      System.out.println(constructString);
      System.out.println("prepare() is invoked in hook");
      System.out.println(conf);
      System.out.print(context);
    }

    @Override
    public void cleanup() {
      GlobalMetrics.incr("hook_cleanup");
      System.out.println("clean() is invoked in hook");
    }

    @Override
    public void emit(EmitInfo info) {
      GlobalMetrics.incr("hook_emit");
      ++emitted;
      if (emitted % N == 0) {
        System.out.println("emit() is invoked in hook");
        System.out.println(info.getValues());
        System.out.println(info.getTaskId());
        System.out.println(info.getStream());
        System.out.println(info.getOutTasks());
      }
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
      GlobalMetrics.incr("hook_spoutAck");
      ++spoutAcked;
      if (spoutAcked % N == 0) {
        System.out.println("spoutAck() is invoked in hook");
        System.out.println(info.getCompleteLatency().toMillis());
        System.out.println(info.getMessageId());
        System.out.println(info.getSpoutTaskId());
      }
    }

    @Override
    public void spoutFail(SpoutFailInfo info) {
      GlobalMetrics.incr("hook_spoutFail");
      ++spoutFailed;
      if (spoutFailed % N == 0) {
        System.out.println("spoutFail() is invoked in hook");
        System.out.println(info.getFailLatency().toMillis());
        System.out.println(info.getMessageId());
        System.out.println(info.getSpoutTaskId());
      }
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
      GlobalMetrics.incr("hook_boltExecute");
      ++boltExecuted;
      if (boltExecuted % N == 0) {
        System.out.println("boltExecute() is invoked in hook");
        System.out.println(info.getExecuteLatency().toMillis());
        System.out.println(info.getExecutingTaskId());
        System.out.println(info.getTuple());
      }
    }

    @Override
    public void boltAck(BoltAckInfo info) {
      GlobalMetrics.incr("hook_boltAck");
      ++boltAcked;
      if (boltAcked % N == 0) {
        System.out.println("boltAck() is invoked in hook");
        System.out.println(info.getAckingTaskId());
        System.out.println(info.getProcessLatency().toMillis());
        System.out.println(info.getTuple());
      }
    }

    @Override
    public void boltFail(BoltFailInfo info) {
      GlobalMetrics.incr("hook_boltFail");
      ++boltFailed;
      if (boltFailed % N == 0) {
        System.out.println("boltFail() is invoked in hook");
        System.out.println(info.getFailingTaskId());
        System.out.println(info.getFailLatency().toMillis());
        System.out.println(info.getTuple());
      }
    }
  }
}
