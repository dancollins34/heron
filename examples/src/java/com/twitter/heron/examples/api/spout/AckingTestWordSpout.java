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
import com.twitter.heron.api.utils.Utils;

public class AckingTestWordSpout extends BaseRichSpout {

  private static final long serialVersionUID = -630307949908406294L;
  private SpoutOutputCollector collector;
  private String[] words;
  private Random rand;
  public int ackCount = 0;
  public int failCount = 0;
  private final long delay;

  public AckingTestWordSpout() {
    this(0);
  }

  public AckingTestWordSpout(long delayMillis){
    this.delay = delayMillis;
  }

  @SuppressWarnings("rawtypes")
  public void open(
      Map conf,
      TopologyContext context,
      SpoutOutputCollector acollector) {
    collector = acollector;
    words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
    rand = new Random();
  }

  public void close() {
  }

  public void nextTuple() {
    Utils.sleep(delay);
    final String word = words[rand.nextInt(words.length)];

    // To enable acking, we need to emit each tuple with a MessageId, which is an Object.
    // Each new message emitted needs to be annotated with a unique ID, which allows
    // the spout to keep track of which messages should be acked back to the producer or
    // retried when the appropriate ack/fail happens. For the sake of simplicity here,
    // however, we'll tag all tuples with the same message ID.
    collector.emit(new Values(word), "MESSAGE_ID");
  }

  // Specifies what happens when an ack is received from downstream bolts
  public void ack(Object msgId) {
    this.ackCount++;
  }

  // Specifies what happens when a tuple is failed by a downstream bolt
  public void fail(Object msgId) {
    this.failCount++;
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }
}
