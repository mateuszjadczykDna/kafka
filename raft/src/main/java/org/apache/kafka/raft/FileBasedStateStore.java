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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.generated.QuorumStateData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.OptionalInt;

/**
 * Local file based quorum state store. It takes the JSON format of {@link QuorumStateData}
 * with an extra data version number as part of the data for easy deserialization.
 *
 * Example format:
 * <pre>
 * {"clusterId":"",
 *   "leaderId":1,
 *   "leaderEpoch":2,
 *   "votedId":-1,
 *   "appliedOffset":0,
 *   "currentVoters":[],
 *   "targetVoters":[],
 *   "data_version":0}
 * </pre>
 * */
public class FileBasedStateStore implements QuorumStateStore {
    private static final Logger log = LoggerFactory.getLogger(FileBasedStateStore.class);

    private final File stateFile;

    static final String DATA_VERSION = "data_version";

    public FileBasedStateStore(final File stateFile) {
        this.stateFile = stateFile;
    }

    private QuorumStateData loadState(File file) throws IOException {
        if (!file.exists()) {
            throw new NoSuchFileException("State store file " + file.toPath() + " is not found");
        }

        try (final BufferedReader reader = Files.newBufferedReader(file.toPath())) {
            final String line = reader.readLine();
            if (line == null) {
                throw new EOFException("File ended prematurely.");
            }

            final ObjectMapper objectMapper = new ObjectMapper();
            JsonNode readNode = objectMapper.readTree(line);

            if (!(readNode instanceof ObjectNode)) {
                throw new IOException("Deserialized node " + readNode.toString() +
                    " is not an object node");
            }
            final ObjectNode dataObject = (ObjectNode) readNode;

            JsonNode dataVersionNode = dataObject.get(DATA_VERSION);
            if (dataVersionNode == null) {
                throw new IOException("Deserialized node " + readNode.toString() +
                    " does not have 'data_version' field");
            }

            final short dataVersion = dataVersionNode.shortValue();
            QuorumStateData data = new QuorumStateData();
            data.fromJson(dataObject, dataVersion);
            return data;
        }
    }

    /**
     * Reads the election state from local file.
     */
    @Override
    public ElectionState readElectionState() throws IOException {
        QuorumStateData data = loadState(stateFile);

        return new ElectionState(data.leaderEpoch(),
            data.leaderId() == UNKNOWN_LEADER_ID ? OptionalInt.empty() :
                OptionalInt.of(data.leaderId()),
            data.votedId() == NOT_VOTED ? OptionalInt.empty() :
                OptionalInt.of(data.votedId()));
    }

    @Override
    public void writeElectionState(ElectionState latest) throws IOException {
        QuorumStateData data = new QuorumStateData()
            .setLeaderEpoch(latest.epoch)
            .setVotedId(latest.hasVoted() ? latest.votedId() : NOT_VOTED)
            .setLeaderId(latest.hasLeader() ? latest.leaderId() : UNKNOWN_LEADER_ID);
        writeElectionStateToFile(stateFile, data);
    }

    private void writeElectionStateToFile(final File stateFile, QuorumStateData state) throws IOException  {
        final File temp = new File(stateFile.getAbsolutePath() + ".tmp");
        Files.deleteIfExists(temp.toPath());

        log.trace("Writing tmp quorum state {}", temp.getAbsolutePath());

        try (final FileOutputStream fileOutputStream = new FileOutputStream(temp);
             final BufferedWriter writer = new BufferedWriter(
                 new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
            short version = state.highestSupportedVersion();

            ObjectNode jsonState = (ObjectNode) state.toJson(version);
            jsonState.set(DATA_VERSION, new ShortNode(version));
            writer.write(jsonState.toString());
            writer.flush();
            fileOutputStream.getFD().sync();
            Utils.atomicMoveWithFallback(temp.toPath(), stateFile.toPath());
        } finally {
            // cleanup the temp file when the write finishes (either success or fail).
            Files.deleteIfExists(temp.toPath());
        }
    }

    /**
     * Clear state store by deleting the local quorum state file
     *
     * @throws IOException if there is any IO exception during delete
     */
    @Override
    public void clear() throws IOException {
        Files.deleteIfExists(stateFile.toPath());
    }

    @Override
    public String toString() {
        return "Quorum state filepath: " + stateFile.getAbsolutePath();
    }
}
