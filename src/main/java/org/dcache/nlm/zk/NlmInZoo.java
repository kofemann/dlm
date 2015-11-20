/*
 * Copyright (c) 2015 Deutsches Elektronen-Synchroton,
 * Member of the Helmholtz Association, (DESY), HAMBURG, GERMANY
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.nlm.zk;

import java.util.List;

import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.dcache.nlm.LockException;
import org.dcache.nlm.LockManager;
import org.dcache.nlm.NlmLock;

import org.dcache.nlm.LockDeniedException;
import org.dcache.nlm.LockRangeUnavailabeException;

import static org.apache.curator.utils.ZKPaths.makePath;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *
 * @author tigran
 */
public class NlmInZoo implements LockManager {

    private final String NLM_ZK_PATH = "/nlm";
    private final String LOCK_PREFIX = "nlm-lock-";

    private final CuratorFramework zkClient;

    public NlmInZoo(String zooLocation) throws Exception {

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zooLocation, retryPolicy);
        zkClient.start();
        zkClient.blockUntilConnected();
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(NLM_ZK_PATH);
    }

    public void shutdown() {
        zkClient.close();
    }

    @Override
    public void lock(byte[] objId, NlmLock lock) throws LockException {
        String nodePath = makePath(NLM_ZK_PATH, "files", BaseEncoding.base16().encode(objId));
        InterProcessSemaphoreMutex zkLock = new InterProcessSemaphoreMutex(zkClient, nodePath);

        try {
            zkLock.acquire();

            // at this point node must exist
            List<String> existingLocks = zkClient.getChildren()
                    .forPath(nodePath);

            Gson gson = new Gson();
            for(String lockNode: existingLocks) {
                if(!lockNode.startsWith(LOCK_PREFIX)) {
                    continue;
                }

                String json = new String(zkClient.getData().forPath(makePath(nodePath, lockNode)), UTF_8);
                NlmLock existingLock = gson.fromJson(json, NlmLock.class);
                if (existingLock.isConflicting(lock)) {
                    throw new LockDeniedException("locked", lock);
                }
            }
            zkClient.create()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(makePath(nodePath, LOCK_PREFIX), gson.toJson(lock).getBytes(UTF_8));
        } catch (Exception e) {
            propagateIfPossible(e);
            propagateIfInstanceOf(e, LockException.class);
            throw new LockException("ZK Exception", e);
        } finally {
            try {
                zkLock.release();
            } catch (Exception e) {
                propagateIfPossible(e);
                throw new LockException("failes to release ZK lock", e);
            }
        }
    }

    @Override
    public void unlock(byte[] objId, NlmLock lock) throws LockException {
        String nodePath = makePath(NLM_ZK_PATH, "files", BaseEncoding.base16().encode(objId));
        InterProcessSemaphoreMutex zkLock = new InterProcessSemaphoreMutex(zkClient, nodePath);

        try {
            zkLock.acquire();

            // at this point node must exist
            List<String> existingLocks = zkClient.getChildren()
                    .forPath(nodePath);

            Gson gson = new Gson();
            for (String lockNode : existingLocks) {

                if (!lockNode.startsWith(LOCK_PREFIX)) {
                    continue;
                }

                String json = new String(zkClient.getData().forPath(makePath(nodePath, lockNode)), UTF_8);
                NlmLock existingLock = gson.fromJson(json, NlmLock.class);
                if (existingLock.equals(lock)) {
                    zkClient.delete().forPath(makePath(nodePath, lockNode));
                    return;
                }
            }

        } catch (Exception e) {
            propagateIfPossible(e);
            propagateIfInstanceOf(e, LockException.class);
            throw new LockException("ZK Exception", e);
        } finally {
            try {
                zkLock.release();
            } catch (Exception e) {
                propagateIfPossible(e);
                throw new LockException("failes to release ZK lock", e);
            }
        }

        throw new LockRangeUnavailabeException("No matching locks");
    }

    @Override
    public void test(byte[] objId, NlmLock lock) throws LockException {
        String nodePath = makePath(NLM_ZK_PATH, "files", BaseEncoding.base16().encode(objId));
        InterProcessSemaphoreMutex zkLock = new InterProcessSemaphoreMutex(zkClient, nodePath);

        try {
            zkLock.acquire();

            // at this point node must exist
            List<String> existingLocks = zkClient.getChildren()
                    .forPath(nodePath);

            Gson gson = new Gson();
            for (String lockNode : existingLocks) {
                if (!lockNode.startsWith(LOCK_PREFIX)) {
                    continue;
                }

                String json = new String(zkClient.getData().forPath(makePath(nodePath, lockNode)), UTF_8);
                NlmLock existingLock = gson.fromJson(json, NlmLock.class);
                if (existingLock.isConflicting(lock)) {
                    throw new LockDeniedException("locked", lock);
                }
            }

        } catch (Exception e) {
            propagateIfPossible(e);
            propagateIfInstanceOf(e, LockException.class);
            throw new LockException("ZK Exception", e);
        } finally {
            try {
                zkLock.release();
            } catch (Exception e) {
                propagateIfPossible(e);
                throw new LockException("failes to release ZK lock", e);
            }
        }
    }
}
