// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.simulator;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.bolt.BaseRichBolt;
import com.twitter.heron.api.bolt.OutputCollector;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.api.tuple.Values;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;

/**
 * Simulator Tester
 */
public class SimulatorTest {

  @SuppressWarnings("unchecked")
  private static void clearSingletonRegistry() throws Exception {
    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);

    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  /**
   * Method: Init()
   */
  @Test
  public void testInit() throws Exception {
    clearSingletonRegistry();
    Simulator spySimulator = Mockito.spy(new Simulator(false));

    spySimulator.init();
    Mockito.verify(spySimulator, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));

    spySimulator.init();
    Mockito.verify(spySimulator, Mockito.times(2)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));
  }

  @Test
  public void testTwoLocaMode() throws Exception {
    clearSingletonRegistry();

    Simulator spySimulator1 = Mockito.spy(new Simulator(false));
    spySimulator1.init();
    Mockito.verify(spySimulator1, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator1, Mockito.times(1)).registerSystemConfig(Mockito.any(SystemConfig.class));

    Simulator spySimulator2 = Mockito.spy(new Simulator(false));
    spySimulator2.init();
    Mockito.verify(spySimulator2, Mockito.times(1)).isSystemConfigExisted();
    Mockito.verify(
        spySimulator2, Mockito.times(0)).registerSystemConfig(Mockito.any(SystemConfig.class));
  }

  @Test
  public void testSimpleTopology() throws Exception {


    int parallelism = 1;
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("word", new WordSpout(), parallelism);
    builder.setBolt("consumer", new ConsumerBolt(), parallelism)
      .fieldsGrouping("word", new Fields("word"));
    Config conf = new Config();
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE);
    conf.setTopologyStatefulCheckpointIntervalSecs(20);

    Simulator simulator = new Simulator();

    simulator.submitTopology("test", conf, builder.createTopology());

    Thread.sleep(30000);

    simulator.stop();
  }
}

class RandomString implements Serializable {
  private static final long serialVersionUID = -209587240589720348L;

  private final char[] symbols;

  private final Random random = new Random();

  private final char[] buf;

  public RandomString(int length) {
    // Construct the symbol set
    StringBuilder tmp = new StringBuilder();
    for (char ch = '0'; ch <= '9'; ++ch) {
      tmp.append(ch);
    }

    for (char ch = 'a'; ch <= 'z'; ++ch) {
      tmp.append(ch);
    }

    symbols = tmp.toString().toCharArray();
    if (length < 1) {
      throw new IllegalArgumentException("length < 1: " + length);
    }

    buf = new char[length];
  }

  public String nextString() {
    for (int idx = 0; idx < buf.length; ++idx) {
      buf[idx] = symbols[random.nextInt(symbols.length)];
    }

    return new String(buf);
  }
}

/**
 * A spout that emits a random word. Note that we dont implement
 * the IStatefulComponent since the spout does not really has any state
 * worth saving. This also illustrates that a Heron topology can
 * have some components that have empty state
 */
class WordSpout extends BaseRichSpout {
  private static final long serialVersionUID = 4322775001819135036L;

  private static final int ARRAY_LENGTH = 128 * 1024;
  private static final int WORD_LENGTH = 20;

  private final String[] words = new String[ARRAY_LENGTH];

  private final Random rnd = new Random(31);

  private SpoutOutputCollector collector;


  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void open(Map map, TopologyContext topologyContext,
                   SpoutOutputCollector spoutOutputCollector) {
    RandomString randomString = new RandomString(WORD_LENGTH);
    for (int i = 0; i < ARRAY_LENGTH; i++) {
      words[i] = randomString.nextString();
    }

    collector = spoutOutputCollector;
  }

  @Override
  public void nextTuple() {
    int nextInt = rnd.nextInt(ARRAY_LENGTH);
    collector.emit(new Values(words[nextInt]));
  }
}

/**
 * A bolt that counts the words that it receives
 */
class ConsumerBolt extends BaseRichBolt
        implements IStatefulComponent<String, Integer> {
  private static final long serialVersionUID = -5470591933906954522L;

  private OutputCollector collector;
  private Map<String, Integer> countMap;
  private State<String, Integer> myState;

  @Override
  public void initState(State<String, Integer> state) {
    this.myState = state;
  }

  @Override
  public void preSave(String checkpointId) {
    // Nothing really since we operate out of the system supplied state
  }

  @SuppressWarnings("rawtypes")
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple) {
    String key = tuple.getString(0);
    if (myState.get(key) == null) {
      myState.put(key, 1);
    } else {
      Integer val = myState.get(key);
      myState.put(key, ++val);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }
}
