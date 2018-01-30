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
package com.twitter.heron.simulator;

import java.io.Serializable;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.state.State;
import com.twitter.heron.api.topology.IStatefulComponent;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;

public class StateAnalysis {
  @Override
  public void persistState(String checkpointId) {
    LOG.info("Persisting state for checkpoint: " + checkpointId);

    if (!isTopologyStateful) {
      throw new RuntimeException("Could not save a non-stateful topology's state");
    }

    // Checkpoint
    if (bolt instanceof IStatefulComponent) {
      ((IStatefulComponent) bolt).preSave(checkpointId);
    }

    collector.sendOutState(instanceState, checkpointId);
  }

  public void sendOutState(State<Serializable, Serializable> state,
                           String checkpointId) {
    outputter.sendOutState(state, checkpointId);
  }

  public void sendOutState(State<Serializable, Serializable> state,
                           String checkpointId) {
    // flush all the current data before sending the state
    flushRemaining();

    // Serialize the state
    byte[] serializedState = serializer.serialize(state);

    // Construct the instance state checkpoint
    CheckpointManager.InstanceStateCheckpoint instanceState =
        CheckpointManager.InstanceStateCheckpoint.newBuilder()
            .setCheckpointId(checkpointId)
            .setState(ByteString.copyFrom(serializedState))
            .build();

    CheckpointManager.StoreInstanceStateCheckpoint storeRequest =
        CheckpointManager.StoreInstanceStateCheckpoint.newBuilder()
            .setState(instanceState)
            .build();

    // Put the checkpoint to out stream queue
    outQueue.offer(storeRequest);
  }
}
