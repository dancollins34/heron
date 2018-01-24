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
import java.util.Random;

import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.examples.api.ExampleResources;

public class FastRandomSentenceSpout extends BaseRichSpout {
  private static final long serialVersionUID = 8068075156393183973L;

  private static final int ARRAY_LENGTH = 128 * 1024;
  private static final int WORD_LENGTH = 20;
  private static final int SENTENCE_LENGTH = 10;

  // Every sentence would be words generated randomly, split by space
  private final String[] sentences = new String[ARRAY_LENGTH];

  private final Random rnd = new Random(31);

  private SpoutOutputCollector collector;

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("sentence"));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    ExampleResources.RandomString randomString = new ExampleResources.RandomString(WORD_LENGTH);
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      StringBuilder sb = new StringBuilder(randomString.nextString());
      for (int j = 1; j < SENTENCE_LENGTH; j++) {
        sb.append(" ");
        sb.append(randomString.nextString());
      }
      sentences[i] = sb.toString();
    }

    collector = spoutOutputCollector;
  }

  @Override
  public void nextTuple() {
    int nextInt = rnd.nextInt(ARRAY_LENGTH);
    collector.emit(new Values(sentences[nextInt]));
  }
}



