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
package com.twitter.heron.examples.api.spout;

import java.util.Map;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;

public class StaticSentenceSpout extends BaseRichSpout {
  private static final long serialVersionUID = 2879005791639364028L;
  private SpoutOutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("sentence"));
  }

  @Override
  @SuppressWarnings({"rawtypes", "HiddenField"})
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    collector = spoutOutputCollector;
  }

  @Override
  public void nextTuple() {
    collector.emit(new Values("Mary had a little lamb"));
  }
}
