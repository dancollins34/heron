//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.examples.api.bolt;

import java.util.Map;

import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;

public class ExclamationBolt extends BaseRichBolt {
  private static final long serialVersionUID = -2267338658317778214L;
  private OutputCollector collector;
  private long nItems;
  private long startTime;
  private final boolean shouldAck;
  private final long modulo;
  private final boolean shouldEmit;
  private final boolean shouldFail;

  public ExclamationBolt(boolean shouldAck, long modulo, boolean shouldEmit, boolean shouldFail){
    this.shouldAck = shouldAck;
    this.modulo = modulo;
    this.shouldEmit = shouldEmit;
    this.shouldFail = shouldFail;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map conf, TopologyContext context, OutputCollector acollector) {
    collector = acollector;
    nItems = 0;
    startTime = System.currentTimeMillis();
  }

  @Override
  public void execute(Tuple tuple) {
    // We need to ack a tuple when we deem that some operation has successfully completed.
    // Tuples can also be failed by invoking collector.fail(tuple)
    // If we do not explicitly ack or fail the tuple after MessageTimeout seconds, which
    // can be set in the topology config, the spout will automatically fail the tuple
    ++nItems;

    if(shouldEmit){
      collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
    }

    if (nItems % 10000 == 0) {
      long latency = System.currentTimeMillis() - startTime;
      System.out.println("Bolt processed " + nItems + " tuples in " + latency + " ms");
      GlobalMetrics.incr("selected_items");
      // Here we explicitly forget to do the ack or fail
      // It would trigger fail on this tuple on spout end after MessageTimeout Seconds
    } else if (this.shouldAck && this.shouldFail){
      if (nItems % 2 == 0) {
        collector.fail(tuple);
      } else {
        collector.ack(tuple);
      }
    } else if (this.shouldAck) {
      collector.ack(tuple);
    } else {
      collector.fail(tuple);
    }


  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    if (shouldEmit) {
      declarer.declare(new Fields("word"));
    }
  }
}