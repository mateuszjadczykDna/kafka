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
package org.apache.kafka.raft;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import org.apache.kafka.raft.generated.QuorumStateData;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;

import static org.apache.kafka.raft.FileBasedStateStore.DATA_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileBasedStateStoreTest {

    private FileBasedStateStore stateStore;

    @Test
    public void testReadElectionState() throws IOException {
        final int leaderId = 1;
        final int epoch = 2;

        final QuorumStateData data = new QuorumStateData()
            .setLeaderId(leaderId)
            .setLeaderEpoch(epoch);

        final File stateFile = TestUtils.tempFile();
        try (final FileOutputStream fileOutputStream = new FileOutputStream(stateFile);
             final BufferedWriter writer = new BufferedWriter(
                 new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
            short version = data.highestSupportedVersion();

            ObjectNode node = (ObjectNode) data.toJson(version);
            node.set(DATA_VERSION, new ShortNode(version));

            writer.write(node.toString());
            writer.flush();
            fileOutputStream.getFD().sync();

            stateStore = new FileBasedStateStore(stateFile);
            assertTrue(stateFile.exists());
            assertEquals(ElectionState.withElectedLeader(epoch, leaderId), stateStore.readElectionState());
        }
    }

    @Test
    public void testWriteElectionState() throws IOException {
        final File stateFile = TestUtils.tempFile();

        stateStore = new FileBasedStateStore(stateFile);

        // We initialized a state from the metadata log
        assertTrue(stateFile.exists());

        // The temp file should be removed
        final File createdTempFile = new File(stateFile.getAbsolutePath() + ".tmp");
        assertFalse(createdTempFile.exists());

        final int epoch = 2;
        final int leaderId = 1;
        final int votedId = 5;

        stateStore.writeElectionState(ElectionState.withElectedLeader(epoch, leaderId));

        assertEquals(stateStore.readElectionState(), new ElectionState(epoch,
            OptionalInt.of(leaderId), OptionalInt.empty()));

        stateStore.writeElectionState(ElectionState.withVotedCandidate(epoch, votedId));

        assertEquals(stateStore.readElectionState(), new ElectionState(epoch,
            OptionalInt.empty(), OptionalInt.of(votedId)));

        final FileBasedStateStore rebootStateStore = new FileBasedStateStore(stateFile);

        assertEquals(rebootStateStore.readElectionState(), new ElectionState(epoch,
            OptionalInt.empty(), OptionalInt.of(votedId)));

        stateStore.clear();
        assertFalse(stateFile.exists());
    }

    @After
    public void cleanup() throws IOException {
        if (stateStore != null) {
            stateStore.clear();
        }
    }
}
