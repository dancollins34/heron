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

import java.util.List;

import com.twitter.heron.api.topology.IUpdatable;

public class UpdatableExclamationBolt extends ExclamationBolt implements IUpdatable{
  public UpdatableExclamationBolt(boolean shouldAck, long modulo) {
    super(shouldAck, modulo);
  }

  /**
   * Implementing this method is optional and only necessary if BOTH of the following are true:
   *
   * a.) you plan to dynamically scale your bolt/spout at runtime using 'heron update'.
   * b.) you need to take action based on a runtime change to the component parallelism.
   *
   * Most bolts and spouts should be written to be unaffected by changes in their parallelism,
   * but some must be aware of it. An example would be a spout that consumes a subset of queue
   * partitions, which must be algorithmically divided amongst the total number of spouts.
   * <P>
   * Note that this method is from the IUpdatable Heron interface which does not exist in Storm.
   * It is fine to implement IUpdatable along with other Storm interfaces, but implementing it
   * will bind an otherwise generic Storm implementation to Heron.
   *
   * @param heronTopologyContext Heron topology context.
   */
  @Override
  public void update(com.twitter.heron.api.topology.TopologyContext heronTopologyContext) {
    List<Integer> newTaskIds =
        heronTopologyContext.getComponentTasks(heronTopologyContext.getThisComponentId());
    System.out.println("Bolt updated with new topologyContext. New taskIds: " + newTaskIds);
  }
}
