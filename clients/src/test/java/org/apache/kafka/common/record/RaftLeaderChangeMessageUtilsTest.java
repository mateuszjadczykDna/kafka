/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.record;

import org.apache.kafka.common.message.LeaderChangeMessageData;
import org.apache.kafka.common.message.LeaderChangeMessageData.Voter;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class RaftLeaderChangeMessageUtilsTest {

    @Test
    public void testInvalidControlRecordType() {
        IllegalArgumentException thrown = assertThrows(
            IllegalArgumentException.class, () -> testDeserializeRecord(ControlRecordType.COMMIT));
        assertEquals("Expected LEADER_CHANGE control record type(3), but get 1", thrown.getMessage());
    }

    @Test
    public void testDeserializeByteData() {
        testDeserializeRecord(ControlRecordType.LEADER_CHANGE);
    }

    private void testDeserializeRecord(ControlRecordType controlRecordType) {
        final int leaderId = 1;
        final int voterId = 2;
        LeaderChangeMessageData data = new LeaderChangeMessageData()
                                           .setLeaderId(leaderId)
                                           .setGrantedVoters(Collections.singletonList(
                                               new Voter().setVoterId(voterId)));

        ByteBuffer valueBuffer = ByteBuffer.allocate(256);
        data.toStruct(data.highestSupportedVersion()).writeTo(valueBuffer);
        valueBuffer.flip();

        byte[] keyData = new byte[]{0, 0, 0, (byte) controlRecordType.type};

        DefaultRecord record = new DefaultRecord(
            256, (byte) 0, 0, 0L, 0, ByteBuffer.wrap(keyData),  valueBuffer, null
        );

        LeaderChangeMessageData deserializedData = RaftLeaderChangeMessageUtils.deserialize(record);

        assertEquals(leaderId, deserializedData.leaderId());
        assertEquals(Collections.singletonList(
            new Voter().setVoterId(voterId)), deserializedData.grantedVoters());
    }

    @Test
    public void testGetMessageSize() {
        assertEquals(256, RaftLeaderChangeMessageUtils.getLeaderChangeMessageSize());
    }


}