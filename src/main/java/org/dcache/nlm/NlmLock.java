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
package org.dcache.nlm;

import java.util.Arrays;

public class NlmLock {

    /**
     * Opaque object that identifies the host or process that is holding the
     * lock.
     */
    private final byte[] holder;
    /**
     * Identifies offset where locked region starts
     */
    private final long offset;
    /**
     * The length of locked region.
     */
    private final long length;

    public NlmLock(byte[] holder, long offset, long length) {
        this.holder = holder;
        this.offset = offset;
        this.length = length;
    }

    public byte[] getHolder() {
        return holder;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 23 * hash + Arrays.hashCode(this.holder);
        hash = 23 * hash + (int) (this.offset ^ (this.offset >>> 32));
        hash = 23 * hash + (int) (this.length ^ (this.length >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NlmLock other = (NlmLock) obj;
        if (this.offset != other.offset) {
            return false;
        }
        if (this.length != other.length) {
            return false;
        }
        return Arrays.equals(this.holder, other.holder);
    }

    public boolean isConflicting(NlmLock other) {
        return !Arrays.equals(holder, other.holder)  && other.offset < offset + length;
    }
}
