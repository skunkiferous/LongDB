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
/**
 *
 */
package com.blockwithme.longdb.client.io.csv;

import javax.annotation.ParametersAreNonnullByDefault;

/** Indicate file parsing errors */
@ParametersAreNonnullByDefault
public class ParsingException extends Exception {

    /** serial version id */
    private static final long serialVersionUID = -9055793947633223201L; // $codepro.audit.disable
                                                                        // hidingInheritedFields

    /** Instantiates a new parsing exception. */
    public ParsingException() {
    }

    /** Instantiates a new parsing exception.
     * 
     * @param theMessage
     *        the error message */
    public ParsingException(final String theMessage) {
        super(theMessage);
    }

    /** Instantiates a new parsing exception.
     * 
     * @param theMessage
     *        the error message.
     * @param theCause
     *        the error */
    public ParsingException(final String theMessage, final Throwable theCause) {
        super(theMessage, theCause);
    }
}
