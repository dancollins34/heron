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
package com.twitter.heron.examples.eco;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import static com.twitter.heron.api.utils.Utils.tuple;
@SuppressWarnings({"serial", "rawtypes"})
public class TestNameCounter extends BaseBasicBolt {

  private Map<String, Integer> counts;

  @Override
  public void prepare(Map map, TopologyContext topologyContext) {
    counts = new HashMap<>();
  }


  protected String getTupleValue(Tuple t, int idx) {
    return (String) t.getValues().get(idx);
  }

  public void execute(Tuple input, BasicOutputCollector collector) {
    String word = getTupleValue(input, 0);
    int count = 0;
    if (counts.containsKey(word)) {
      count = counts.get(word);
    }
    count++;
    counts.put(word, count);
    collector.emit(tuple(word, count));
  }

  public void cleanup() {

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("name", "count"));
  }

}
