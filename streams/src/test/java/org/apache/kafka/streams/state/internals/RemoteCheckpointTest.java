package org.apache.kafka.streams.state.internals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;


public class RemoteCheckpointTest {

    private static String dbDirPath;
    private static String backupDirName;
    private static RemoteCheckpoint remoteCheckpoint;
    private static long intervalMs;

    @BeforeClass
    public static void setup() throws Exception {
        RocksDB.loadLibrary();
        dbDirPath =  "/tmp/test_rocksdb_path";
        File dbFile = new File(dbDirPath);
        if (!dbFile.exists()) {
            dbFile.mkdirs();
        }
        Options options = new Options();
        options.setCreateIfMissing(true);
        RocksDB db = RocksDB.open(options, dbFile.getAbsolutePath());
        db.put(new byte[1], new byte[2]);
        FlushOptions fOptions = new FlushOptions();
        fOptions.waitForFlush();
        db.flush(fOptions);
        String applicationId = "appId";
        String taskId = "t1";
        String dbName = "test_db";
        backupDirName = "/tmp/test_rocksdb_backup";
        intervalMs = 30_000L;
        remoteCheckpoint = new RemoteCheckpoint(db, applicationId, taskId, dbName, backupDirName, "s3Bucket", "s3Key", intervalMs);
    }

    @Test
    public void testCreatCopy() throws Exception {
        assertFalse(remoteCheckpoint.shouldCheckpoint());
        remoteCheckpoint.setLastRemoteCheckpointStartTimeMs(System.currentTimeMillis() - 2 * intervalMs);
        assertTrue(remoteCheckpoint.shouldCheckpoint());
        File backupDir = new File(backupDirName);
        assertTrue(backupDir.exists() && backupDir.isDirectory());
        // A second run should be able to clear the local db backup.
        remoteCheckpoint.runOnce();
        String[] files = backupDir.list();
        assertTrue(backupDir.exists() && files != null && files.length == 0);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (!RemoteCheckpoint.deleteDir(new File(dbDirPath)) || !RemoteCheckpoint.deleteDir(new File(backupDirName))) {
           throw new Exception("unable to clean up the db directories");
        }
    }

}