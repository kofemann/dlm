package org.dcache.nlm;

import java.nio.charset.StandardCharsets;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class NlmLockTest {


    private final byte[] OWNER1 = "owner1".getBytes(StandardCharsets.UTF_8);
    private final byte[] OWNER2 = "owner2".getBytes(StandardCharsets.UTF_8);

    @Test
    public void testCheckConflictingByOwnerSameRange() {
        NlmLock lock1 = new NlmLock(OWNER1, 0, 10);
        NlmLock lock2 = new NlmLock(OWNER2, 0, 10);

        assertConflicting(lock1,lock2);
    }

    @Test
    public void testCheckConflictingByOwnerNonOverlapingRange() {
        NlmLock lock1 = new NlmLock(OWNER1, 0, 10);
        NlmLock lock2 = new NlmLock(OWNER2, 10, 11);

        assertNonConflicting(lock1, lock2);
    }

    @Test
    public void testCheckConflictingByOwnerOverlapingRange() {
        NlmLock lock1 = new NlmLock(OWNER1, 0, 10);
        NlmLock lock2 = new NlmLock(OWNER2, 9, 11);

        assertConflicting(lock1, lock2);
    }

    @Test
    public void testCheckConflictingInMiddle() {
        NlmLock lock1 = new NlmLock(OWNER1, 0, 100);
        NlmLock lock2 = new NlmLock(OWNER2, 10, 11);

        assertConflicting(lock1, lock2);
    }

    private void assertConflicting(NlmLock lock1, NlmLock lock2) {
        assertTrue("conflict (1) not detected", lock1.isConflicting(lock2));
        assertTrue("conflict (2) not detected", lock2.isConflicting(lock1));
    }

    private void assertNonConflicting(NlmLock lock1, NlmLock lock2) {
        assertFalse("false (1) conflict", lock1.isConflicting(lock2));
        assertFalse("false (2) conflict", lock2.isConflicting(lock1));
    }

}
