package org.dcache.nlm.zk;

import java.nio.charset.StandardCharsets;
import org.apache.curator.test.TestingServer;
import org.dcache.nlm.LockDeniedException;
import org.dcache.nlm.LockException;
import org.dcache.nlm.LockRangeUnavailabeException;
import org.dcache.nlm.NlmLock;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;

/**
 *
 */
public class NlmInZooTest {

    private TestingServer zkTestServer;
    private NlmInZoo nlm;

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServer();
        nlm = new NlmInZoo(zkTestServer.getConnectString());
    }

    @After
    public void shutDown() throws Exception {
        zkTestServer.stop();
        nlm.shutdown();
    }

    @Test
    public void shouldAllowFreshLock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();
    }

    @Test(expected = LockDeniedException.class)
    public void shouldFailOnConflictingLockDifferentOwner() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner2")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();
    }

    @Test
    public void shouldAllowConflictingLockSameOwner() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();
    }

    @Test
    public void shouldAllowNonConflictingLock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner2")
                .on("file1")
                .from(2)
                .lenggth(1)
                .lock();
    }

    @Test
    public void shouldAllowConflictingLocksOnDifferentFiles() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner2")
                .on("file2")
                .from(0)
                .lenggth(1)
                .lock();
    }

    @Test(expected = LockRangeUnavailabeException.class)
    public void shouldFailOnNonMatchingLock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .unlock();
    }

    @Test
    public void shouldAllowLockAfterUnlock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .unlock();

        given().owner("owner2")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();
    }

    @Test
    public void shouldAllowTestOnNonExinstingLock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .test();
    }

    @Test(expected = LockDeniedException.class)
    public void shouldFailTestOnNonExinstingLock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner2")
                .on("file1")
                .from(0)
                .lenggth(1)
                .test();
    }

    @Test
    public void shouldAllowTestAfterUnLock() throws LockException {
        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .lock();

        given().owner("owner1")
                .on("file1")
                .from(0)
                .lenggth(1)
                .unlock();

        given().owner("owner2")
                .on("file1")
                .from(0)
                .lenggth(1)
                .test();
    }

    private LockBuilder given() {
        return new LockBuilder();
    }

    private class LockBuilder {

        private byte[] file;
        private byte[] holder;
        private long offset;
        private long length;

        LockBuilder on(String file) {
            this.file = file.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        LockBuilder owner(String owner) {
            this.holder = owner.getBytes(StandardCharsets.UTF_8);
            return this;
        }

        LockBuilder from(long offset) {
            this.offset = offset;
            return this;
        }

        LockBuilder lenggth(long length) {
            this.length = length;
            return this;
        }

        void lock() throws LockException {
            NlmLock lock = new NlmLock(holder, offset, length);
            nlm.lock(file, lock);
        }

        void unlock() throws LockException {
            NlmLock lock = new NlmLock(holder, offset, length);
            nlm.unlock(file, lock);
        }

        void test() throws LockException {
            NlmLock lock = new NlmLock(holder, offset, length);
            nlm.test(file, lock);
        }

    }
}
