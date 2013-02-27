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
// $codepro.audit.disable packageStructure
package test;

import java.io.IOException;

import org.json.JSONException;

import com.blockwithme.longdb.BEDatabase;
import com.blockwithme.longdb.BETable;
import com.blockwithme.longdb.Backend;
import com.blockwithme.longdb.discovery.BackendServiceLoader;
import com.blockwithme.longdb.entities.Base36;
import com.blockwithme.longdb.test.util.JSONUtil;
import com.blockwithme.longdb.voltdb.VoltDBConstants;

/** The Test class to Load Table. */
// CHECKSTYLE IGNORE FOR NEXT 1 LINES
public class LoadTable {

    /** The main method.
     * 
     * @param theArgs
     *        the arguments
     * @throws IOException
     *         Signals that an I/O exception has occurred.
     * @throws JSONException
     *         the jSON exception */
    public static void main(final String[] theArgs) throws IOException, // $codepro.audit.disable
                                                                        // illegalMainMethod
            JSONException {
        final Backend be = BackendServiceLoader.getInstance().loadInstance(
                VoltDBConstants.PROJECT_NAME, "default");
        final BEDatabase db = be.openDatabase("default");
        if (db != null) {
            final BETable table = db.get(Base36.get("defaulttable"));
            final JSONUtil util = new JSONUtil("/InputJSON.txt");
            util.loadTableFromJSONFile(table);
        }
    }
}
