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
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Tuple;

public class CountPrintBolt extends BaseRichBolt{
  private static final long serialVersionUID = 1913733461146490337L;
  private long nItems;
  private final int modulo;

  public CountPrintBolt(int modulo){
    this.modulo = modulo;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(
      Map conf,
      TopologyContext context,
      OutputCollector acollector) {
    nItems = 0;
  }

  @Override
  public void execute(Tuple tuple) {
    if (++nItems % modulo == 0) {
      System.out.println(tuple.getString(0));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}
