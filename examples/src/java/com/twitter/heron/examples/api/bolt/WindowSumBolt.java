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

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.api.bolt.BaseWindowedBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.api.windowing.TupleWindow;

public class WindowSumBolt extends BaseWindowedBolt {
  private static final long serialVersionUID = 8458595466693183050L;
  private OutputCollector collector;
  private Map<String, Integer> counts = new HashMap<String, Integer>();

  @Override
  @SuppressWarnings("HiddenField")
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
      collector) {
    this.collector = collector;
  }

  @Override
  public void execute(TupleWindow inputWindow) {
    int sum = counts.getOrDefault("sum", 0);
    for (Tuple tuple : inputWindow.get()) {
      sum += tuple.getIntegerByField("value");
    }
    counts.put("sum", sum);
    collector.emit(new Values(sum));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sum"));
  }
}
