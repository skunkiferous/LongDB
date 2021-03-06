/*******************************************************************************
 * Copyright (c) 2013 Sebastien Diot..
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Public License v3.0
 * which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/gpl.html
 * 
 * Contributors:
 *     Sebastien Diot. - initial API and implementation
 ******************************************************************************/
package com.blockwithme.longdb.voltdb;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/** The class encapsulates VoltDB configuration parameters */
@ParametersAreNonnullByDefault
public class VoltDBConfig {

    /** The host name. */
    private String hostName;

    /** Auto tune flag */
    private boolean autoTune;

    /** The 'heavy weight' flag. */
    private boolean heavyweight;

    /** The connection response timeout. */
    private long connectionResponseTimeout;

    /** The max transactions per second. */
    private int maxTransactionsPerSecond;

    /** The max outstanding transactions. */
    private int maxOutstandingTxns;

    /** The procedure call timeout. */
    private long procedureCallTimeout;

    /** Host name.
     * 
     * @param theHostName
     *        the host name */
    @Inject
    void hostName(@Named("hostName") final String theHostName) {
        this.hostName = theHostName;
    }

    /** Gets the host name.
     * 
     * @return the host name */
    public String hostName() {
        return hostName;
    }

    /** Auto tune flag
     * 
     * @param theAutoTune
     *        the auto tune flag */
    @Inject
    void autoTune(@Named("autoTune") final boolean theAutoTune) {
        this.autoTune = theAutoTune;
    }

    /** Auto tune.
     * 
     * @return the auto tune flag */
    public boolean autoTune() {
        return autoTune;
    }

    /** sets heavy weight flag.
     * 
     * @param theHeavyweight
     *        the is heavy weight flag */
    @Inject
    void heavyweight(@Named("heavyweight") final boolean theHeavyweight) {
        this.heavyweight = theHeavyweight;
    }

    /** @return the heavy weight flag */
    public boolean heavyweight() {
        return heavyweight;
    }

    /** Connection response timeout.
     * 
     * @param theConnectionResponseTimeout
     *        the connection response timeout */
    @Inject
    void connectionResponseTimeout(
            @Named("connectionResponseTimeout") final long theConnectionResponseTimeout) {
        this.connectionResponseTimeout = theConnectionResponseTimeout;
    }

    /** Connection response timeout.
     * 
     * @return the connection response timeout */
    public long connectionResponseTimeout() {
        return connectionResponseTimeout;
    }

    /** Max transactions per second.
     * 
     * @param theMaxTransactionsPerSecond
     *        the max transactions per second */
    @Inject
    void maxTransactionsPerSecond(
            @Named("maxTransactionsPerSecond") final int theMaxTransactionsPerSecond) {
        this.maxTransactionsPerSecond = theMaxTransactionsPerSecond;
    }

    /** Max transactions per second.
     * 
     * @return max transactions per second. */
    public int maxTransactionsPerSecond() {
        return maxTransactionsPerSecond;
    }

    /** Max outstanding transactions.
     * 
     * @param theMaxOutstandingTxns
     *        the max outstanding transactions */
    @Inject
    void maxOutstandingTxns(
            @Named("maxOutstandingTxns") final int theMaxOutstandingTxns) {
        this.maxOutstandingTxns = theMaxOutstandingTxns;
    }

    /** Max outstanding transactions..
     * 
     * @return max outstanding transactions. */
    public int maxOutstandingTxns() {
        return maxOutstandingTxns;
    }

    /** Procedure call timeout.
     * 
     * @param theProcedureCallTimeout
     *        the procedure call timeout */
    @Inject
    void procedureCallTimeout(
            @Named("procedureCallTimeout") final long theProcedureCallTimeout) {
        this.procedureCallTimeout = theProcedureCallTimeout;
    }

    /** Procedure call timeout.
     * 
     * @return the procedure call timeout. */
    public long procedureCallTimeout() {
        return procedureCallTimeout;
    }

}
