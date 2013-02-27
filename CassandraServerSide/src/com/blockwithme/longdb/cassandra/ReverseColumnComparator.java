/*******************************************************************************
 * Copyright 2013 Sebastien Diot
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
// $codepro.audit.disable missingStaticMethod
package com.blockwithme.longdb.cassandra;

import java.nio.ByteBuffer;
import java.util.Comparator;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;

/** Compares Column IDs and returns result in reverse order, This class must be
 * supplied to Cassandra server library so that it can be loaded by Cassandra
 * process. */
@ParametersAreNonnullByDefault
public final class ReverseColumnComparator extends AbstractType<Long> {

    /** The other instance. */
    private static final Comparator<ByteBuffer> OTHER_INSTANCE = LongType.instance.reverseComparator;

    /** The instance this is essential to access the singleton instance of this
     * class. (don't change the name of this static member variable.) */

    // CHECKSTYLE IGNORE FOR NEXT 1 LINES
    public static final ReverseColumnComparator instance = new ReverseColumnComparator(); // $codepro.audit.disable
                                                                                          // constantNamingConvention

    // Singleton
    /** Instantiates a new reverse column comparator. */
    private ReverseColumnComparator() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(final ByteBuffer theFirstBuff,
            final ByteBuffer theSecondBuffer) {
        return OTHER_INSTANCE.compare(theFirstBuff, theSecondBuffer);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.cassandra.db.marshal.AbstractType#compose(java.nio.ByteBuffer)
     */
    @Override
    public Long compose(final ByteBuffer theArg) {
        return LongType.instance.compose(theArg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.cassandra.db.marshal.AbstractType#decompose(java.lang.Object)
     */
    @Override
    public ByteBuffer decompose(final Long theArg) {
        return LongType.instance.decompose(theArg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.cassandra.db.marshal.AbstractType#fromString(java.lang.String)
     */
    @Override
    public ByteBuffer fromString(final String theArg) {
        return LongType.instance.fromString(theArg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.cassandra.db.marshal.AbstractType#getString(java.nio.ByteBuffer
     * )
     */
    @Override
    public String getString(final ByteBuffer theArg) {
        return LongType.instance.getString(theArg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.cassandra.db.marshal.AbstractType#validate(java.nio.ByteBuffer
     * )
     */
    @Override
    public void validate(final ByteBuffer theArg) {
        LongType.instance.validate(theArg);
    }
}
