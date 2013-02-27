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
// $codepro.audit.disable hidingInheritedFields
package com.blockwithme.longdb.exception;

/** Exception represents DB any errors while performing DB operations. */
public class DBException extends RuntimeException {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -7792149841582261783L;

    /** Instantiates a new dB exception. */
    public DBException() {
    }

    /** @param theMessage
     *        the error message */
    public DBException(final String theMessage) {
        super(theMessage);
    }

    /** @param theMessage
     *        the error message
     * @param theCause
     *        the cause of this exception */
    public DBException(final String theMessage, final Throwable theCause) {
        super(theMessage, theCause);
    }

    /** @param theCause
     *        the cause of this exception */
    public DBException(final Throwable theCause) {
        super(theCause);
    }
}
