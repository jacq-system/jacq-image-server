/*
JACQ
Copyright (C) 2011-2013 Naturhistorisches Museum Wien

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package at.ac.nhm_wien.jacq;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.regex.Matcher;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author wkoller
 */
public class ImageServer extends HttpServlet {

    private static final long serialVersionUID = 1L;

    /**
     * Used by all classes to access the configuration settings
     */
    public static Properties m_properties = new Properties();

    //private String m_requestId = "";
    //private JSONArray m_requestParams = null;
    //private JSONObject m_response = null;
    private Connection m_conn = null;

    private ImportThread m_importThread = null;
    private ExportThread m_exportThread = null;

    /**
     * Initialize Servlet
     *
     * @throws ServletException
     */
    @Override
    public void init() throws ServletException {
        try {
            // Load properties
            m_properties.load(Files.newInputStream(Paths.get(getServletContext().getRealPath("/WEB-INF/" + this.getClass().getSimpleName() + ".properties"))));
            // Establish "connection" to SQLite database
            m_conn = Utilities.getConnection();

            // Check if database is already initialized
            Statement stat = m_conn.createStatement();
            // Check for resources table
            ResultSet rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'resources' AND type = 'table'");
            if (!rs.next()) {
                StringBuilder builder = new StringBuilder("CREATE table `resources` ( `r_id` INTEGER CONSTRAINT `r_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT CONSTRAINT `identifier_unique` UNIQUE ON CONFLICT FAIL, `imageFile` TEXT");
                if(!Utilities.isLegacy(m_properties)) {
                    builder.append(", `public` INTEGER DEFAULT 1 ");
                }

                builder.append(")");

                stat.executeUpdate(builder.toString());
            }
            rs.close();
            stat.close();
            // Check for archive resources table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'archive_resources' AND type = 'table'");
            if (!rs.next()) {
                stat.executeUpdate("CREATE table `archive_resources` ( `ar_id` INTEGER CONSTRAINT `ar_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT, `imageFile` TEXT, `lastModified` INTEGER DEFAULT 0, `size` INTEGER DEFAULT 0, `obsolete` INTEGER DEFAULT 0, `it_id` INTEGER )");
            }
            rs.close();
            stat.close();
            // Check for import queue table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_queue' AND type = 'table'");
            if (!rs.next()) {
                stat.executeUpdate("CREATE table `import_queue` ( `iq_id` INTEGER CONSTRAINT `iq_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT, `filePath` TEXT, `force` INTEGER DEFAULT 0 )");
            }
            rs.close();
            stat.close();
            // Check for export queue table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'export_queue' AND type = 'table'");
            if (!rs.next()) {
                stat.executeUpdate("CREATE table `export_queue` ( `eq_id` INTEGER CONSTRAINT `eq_id_pk` PRIMARY KEY AUTOINCREMENT, `archiveFilePath` TEXT, `exportFilePath` TEXT )");
            }
            rs.close();
            stat.close();
            // Check for threads table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'threads' AND type = 'table'");
            if (!rs.next()) {
                stat.executeUpdate("CREATE table `threads` ( `t_id` INTEGER CONSTRAINT `t_id_pk` PRIMARY KEY AUTOINCREMENT, `thread_id` INTEGER, `starttime` INTEGER DEFAULT 0, `endtime` INTEGER DEFAULT 0, `type` INTEGER DEFAULT 0 )");
            }
            rs.close();
            stat.close();
            // Check for threads logging table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'thread_logs' AND type = 'table'");
            if (!rs.next()) {
                stat.executeUpdate("CREATE table `thread_logs` ( `tl_id` INTEGER CONSTRAINT `tl_id_pk` PRIMARY KEY AUTOINCREMENT, `t_id` INTEGER, `logtime` INTEGER DEFAULT 0, `identifier` TEXT DEFAULT '', `message` TEXT )");
            }
            rs.close();
            stat.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    @Override
    public void destroy() {
        try {
            m_conn.close();
        } catch (Exception e) {
            System.err.println("Unable to close connection to SQLite: " + e.getMessage());
        }

        super.destroy();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if (req.getParameter("request") != null) {
            resp.getOutputStream().print(handleRequest(new JSONObject(req.getParameter("request"))));
        }
        // Quite hacky way to support Mootools interpretation of a JSON-RPC call
        else {
            JSONObject reqObject = new JSONObject();
            JSONArray params = new JSONArray();

            Map<String, String[]> parameters = req.getParameterMap();

            Iterator<Map.Entry<String, String[]>> paramsIt = parameters.entrySet().iterator();
            while (paramsIt.hasNext()) {
                Map.Entry<String, String[]> currEntry = paramsIt.next();

                // Id & Method are handled special
                if (currEntry.getKey().equals("id") || currEntry.getKey().equals("method")) {
                    reqObject.put(currEntry.getKey(), currEntry.getValue()[0]);
                }
                else // Check if we have passed an array as parameter
                {
                    if (currEntry.getValue().length > 1) {
                        params.put(new JSONArray(currEntry.getValue()));
                    }
                    else {
                        params.put(currEntry.getValue()[0]);
                    }
                }
            }

            // Finally add the parameter as well
            reqObject.put("params", params);

            // Run the query
            resp.getOutputStream().print(handleRequest(reqObject));
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String inputString = "";
        BufferedReader in = new BufferedReader(new InputStreamReader(req.getInputStream()));
        for (String buffer; (buffer = in.readLine()) != null; inputString += buffer + "\n");

        resp.getOutputStream().print(handleRequest(new JSONObject(inputString)));
    }

    /**
     * Handle an incoming request (already parsed as JSON-RPC request)
     *
     * @param reqObject Contains the request object
     * @return The output which should be sent to the client
     */
    protected String handleRequest(JSONObject reqObject) {
        // prepare default response object
        JSONObject response = new JSONObject();
        response.put("error", (Map<?, ?>) null);
        response.put("result", (Map<?, ?>) null);
        String methodName = "";
        if (!reqObject.isEmpty()) {
            try {
                try {
                    // Fetch passed parameters
                    response.put("id", reqObject.getString("id"));
                    JSONArray requestParams = reqObject.getJSONArray("params");
                    methodName = reqObject.getString("method");

                    // Check if we have at least one key
                    if (requestParams.isEmpty()) {
                        throw new Exception("No key passed");
                    }

                    // First parameter always MUST be the authentication key
                    String requestKey = requestParams.getString(0);
                    requestParams.remove(0);

                    // Check if requestKey is valid
                    if (!requestKey.equals(m_properties.getProperty("ImageServer.key"))) {
                        throw new Exception("Invalid key passed");
                    }

                    // Find correct method to call
                    Method callMethod = null;
                    Method[] methods = this.getClass().getMethods();
                    for (Method method : methods) {
                        // Prefix method with 'x_' to preven arbitrary executions
                        if (method.getName().equals("x_" + methodName)) {
                            // Check if the parameters do fit
                            @SuppressWarnings("rawtypes")
                            Class[] parameterTypes = method.getParameterTypes();

                            // Check if we have parameters
                            if (requestParams.isEmpty() && parameterTypes.length == 0) {
                                callMethod = method;
                                response.put("result", callMethod.invoke(this));
                                break;
                            }
                            else if (requestParams.length() > 0 && parameterTypes.length > 0 && parameterTypes[0] == JSONArray.class) {
                                callMethod = method;
                                response.put("result", callMethod.invoke(this, requestParams));
                                break;
                            }
                        }
                    }

                    // Check if we found a method at all
                    if (callMethod == null) {
                        throw new Exception("Method not found");
                    }
                } catch (InvocationTargetException ite) {
                    throw ite.getCause();
                }
            } catch (Throwable e) {
                response.put("error", "Unable to call requested method ('" + methodName + "'): " + e.getMessage() + " / " + e.toString());
            }
        }

        return response.toString();
    }

    /**
     * PUBLIC FUNCTIONS START
     */
    /**
     * Starts a thread for importing new images
     */
    public int x_importImages() throws Exception {
        // Check if importthread is still active
        if (m_importThread != null && !m_importThread.isAlive()) {
            m_importThread = null;
        }

        // Check if thread is already running
        if (m_importThread == null) {
            try {
                // Start new import thread
                m_importThread = new ImportThread();
                m_importThread.start();

                // Return thread id
                return m_importThread.getThread_id();
            } // Something went wrong during thread startup
            catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace();

                throw new Exception("Error while trying to start thread: " + e.getMessage());
            }
        }
        else {
            throw new Exception("Thread already running!");
        }
    }

    /**
     * Wheter if we want the obsoletes can be passed as first parameter
     *
     * @param params
     */
    public JSONArray x_listArchiveImages(JSONArray params) throws Exception {
        return this.listArchiveImages((params.getBoolean(0)) ? 1 : 0);
    }

    /**
     * Calling listArchiveImages without parameters is allowed as well
     */
    public JSONArray x_listArchiveImages() throws Exception {
        return this.listArchiveImages(0);
    }

    /**
     * List all images currently stored in the archive
     *
     * @param p_obsolete return obsolete entries
     */
    private JSONArray listArchiveImages(int p_obsolete) throws Exception {
        try {
            JSONArray resources = new JSONArray();

            PreparedStatement prepStat = m_conn.prepareStatement("SELECT `identifier` FROM `archive_resources` WHERE `obsolete` = ? ORDER BY `identifier`");
            prepStat.setInt(1, p_obsolete);
            ResultSet rs = prepStat.executeQuery();

            while (rs.next()) {
                resources.put(rs.getString("identifier"));
            }
            rs.close();
            prepStat.close();

            return resources;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * List all images currently stored in the archive
     */
    public JSONArray x_listDjatokaImages() throws Exception {
        try {
            JSONArray resources = new JSONArray();

            Statement stat = m_conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT `identifier` FROM `resources` ORDER BY `identifier`");
            while (rs.next()) {
                resources.put(rs.getString("identifier"));
            }
            rs.close();
            stat.close();

            return resources;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Get a list of threads (after a certain date, optionally filtered by type)
     */
    public JSONObject x_listThreads(JSONArray params) throws Exception {
        if (params.length() > 1) {
            return listThreads(params.getInt(0), params.getInt(1));
        }
        // By default do not filter by thread type
        else {
            return listThreads(params.getInt(0), 0);
        }
    }

    /**
     * List all import threads
     *
     * @param cutoff_date threads older than cutoff_date wont be returned
     * @param type limit returned threads to a certain type
     */
    private JSONObject listThreads(int cutoff_date, int type) throws Exception {
        try {
            JSONObject threads = new JSONObject();

            // Prepare statement
            PreparedStatement stat = null;
            // Check if we have to filter by type
            if (type > 0) {
                stat = m_conn.prepareStatement("SELECT `t_id`, `starttime`, `endtime`, `type` FROM `threads` WHERE `starttime` >= ? AND `type` = ? ORDER BY `t_id` DESC");
                stat.setInt(2, type);
            }
            else {
                stat = m_conn.prepareStatement("SELECT `t_id`, `starttime`, `endtime`, `type` FROM `threads` WHERE `starttime` >= ? ORDER BY `t_id` DESC");
            }
            stat.setInt(1, cutoff_date);

            // Fetch all fitting threads
            ResultSet rs = stat.executeQuery();
            while (rs.next()) {
                // Combine thread related information in an object
                JSONObject threadInfo = new JSONObject();
                threadInfo.put("starttime", rs.getInt("starttime"));
                threadInfo.put("endtime", rs.getInt("endtime"));
                threadInfo.put("type", rs.getInt("type"));

                // Add thread info to stack
                threads.put(rs.getString("t_id"), threadInfo);
            }
            rs.close();
            stat.close();

            return threads;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Returns a list of log messages for a given thread-id
     */
    public JSONArray x_listImportLogs(JSONArray params) throws Exception {
        return listImportLogs(params.getInt(0));
    }

    /**
     * Returns a list of log messages for a given thread-id
     *
     * @param it_id
     */
    private JSONArray listImportLogs(int it_id) throws Exception {
        try {
            JSONArray logs = new JSONArray();

            PreparedStatement stat = m_conn.prepareStatement("SELECT `logtime`, `identifier`, `message` FROM `thread_logs` WHERE `t_id` = ? ORDER BY `logtime` ASC");
            stat.setString(1, String.valueOf(it_id));
            ResultSet rs = stat.executeQuery();
            while (rs.next()) {
                JSONObject logObj = new JSONObject();
                logObj.put("logtime", rs.getString("logtime"));
                logObj.put("identifier", rs.getString("identifier"));
                logObj.put("message", rs.getString("message"));
                logs.put(logObj);
            }
            rs.close();
            stat.close();

            return logs;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Start exporting images (callable function)
     */
    public int x_exportImages(JSONArray params) throws Exception {
        return exportImages("", params.getJSONArray(0));
    }

    /**
     * Start exporting images
     *
     * @param p_exportPath relative path inside the exportDirectory property
     * @param p_identifiers list of identifiers to export
     */
    private int exportImages(String p_exportPath, JSONArray p_identifiers) throws Exception {
        // Check if thread is still alive
        if (m_exportThread != null && !m_exportThread.isAlive()) {
            m_exportThread = null;
        }

        // Check for export thread
        if (m_exportThread == null) {
            try {
                // Construct export path
                String exportPath = m_properties.getProperty("ImageServer.exportDirectory");

                // Replace any ../ with an empty string
                // Note: this regular expression does not treat ../ very cleanly,
                // however we only want to prevent overwriting anything outside the base-path
                p_exportPath = p_exportPath.replaceAll("\\.\\./", "");

                // Check for trailing slash
                if (!exportPath.endsWith("/")) {
                    exportPath += "/";
                }

                // Create new export thread
                m_exportThread = new ExportThread(exportPath + p_exportPath, p_identifiers);
                m_exportThread.start();

                // We finished successfully if we reach here
                return m_exportThread.getThread_id();
            } catch (Exception e) {
                throw new Exception(e.getMessage());
            }
        }
        // ... thread already running
        else {
            throw new Exception("Export thread already running");
        }
    }

    /**
     * Return all found resources for a given list of identifiers
     *
     * @param params
     */
    public JSONArray x_listResources(JSONArray params) throws Exception {
        try {
            // fetch list of resources
            JSONArray identifiers = params.getJSONArray(0);

            // check if (optional) second parameter is present
            boolean bPublicOnly = false;
            if (params.length() > 1) {
                if (params.getBoolean(1)) {
                    bPublicOnly = true;
                }
            }

            // in order to prevent SQLLite from going mad we make the calls in 100 unit steps
            int fromIndex = 0;
            JSONArray resources = new JSONArray();
            while (fromIndex < identifiers.length()) {
                int toIndex = fromIndex + 100;

                // check for out of bound
                if (toIndex > identifiers.length()) {
                    toIndex = identifiers.length();
                }

                // fetch next list and add it to resources
                resources.putAll(this.listResources(new JSONArray(Utilities.subJsonArray(identifiers, fromIndex, toIndex)), bPublicOnly));
                fromIndex = toIndex;

                System.err.println("processed: " + toIndex + " / " + identifiers.length());
            }

            // prepare response
            return resources;
        } catch (Exception e) {
            // in case of an error, return it
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Gets passed a list of resource-identifiers which are then searched for
     *
     * @param identifiers List of identifiers to search for
     */
    private JSONArray listResources(JSONArray identifiers, boolean bPublicOnly) throws Exception {
        JSONArray resources = new JSONArray();

        // Check if we have at least one identifier
        if (identifiers.length() < 1) {
            throw new Exception("No identifiers passed");
        }

        // Convert conditions to ArrayList
        ArrayList<String> conditions = new ArrayList<String>();
        for (int i = 0; i < identifiers.length(); i++) {
            conditions.add("`identifier` LIKE ? ESCAPE '\\'");
        }
        // Create plain string where condition
        String whereConditions = String.join(" OR ", conditions);

        String requestedFields = "`identifier`";

        // Apply permissions if not in legacy mode
        if(!Utilities.isLegacy(m_properties)) {
            requestedFields += ", `public`";
            // check if only public entries should be returned
            if (bPublicOnly) {
                whereConditions = "(" + whereConditions + ") AND `public` = 1";
            }
        }

        // Prepare the actual statement
        PreparedStatement stat = m_conn.prepareStatement("SELECT " + requestedFields + " FROM `resources` WHERE " + whereConditions + " ORDER BY `identifier` ASC");
        for (int i = 0; i < identifiers.length(); i++) {
            // Add string (but make sure the underscore is masked)
            stat.setString(i + 1, identifiers.getString(i).replaceAll("_", Matcher.quoteReplacement("\\_")));
        }

        // Execute the query & fetch all found files
        ResultSet rs = stat.executeQuery();
        while (rs.next()) {
            if(Utilities.isLegacy(m_properties)) {
                resources.put(rs.getString("identifier"));
            }
            else {
                // fetch resource details and add them as object
                JSONObject resourceDetails = new JSONObject();
                resourceDetails.put("identifier", rs.getString("identifier"));
                resourceDetails.put("public", rs.getString("public"));

                // add to return list
                resources.put(resourceDetails);
            }
        }
        rs.close();
        stat.close();

        return resources;
    }

    /**
     * Returns a list of file identifiers for a given specimen
     *
     * @deprecated use listResources instead
     */
    public JSONArray x_listSpecimenImages(JSONArray params) throws Exception {
        if (params.length() >= 3 && params.getBoolean(2)) {
            return listSpecimenImages(params.getInt(0), params.getString(1), true);
        }
        else {
            return listSpecimenImages(params.getInt(0), params.getString(1), false);
        }
    }

    /**
     * Returns a list of file identifiers for a given specimen
     *
     * @deprecated use listResources instead
     * @param specimen_id Specimen ID
     * @param herbar_number Herbarnumber of specimen
     * @param excludeTabObs Exclude tab and obs entries
     */
    private JSONArray listSpecimenImages(int specimen_id, String herbar_number, boolean excludeTabObs) throws Exception {
        try {
            // Cleanup passed herbar_number
            herbar_number = herbar_number.replaceAll("%", "\\%");

            // Create all possible variants of filenaming
            JSONArray identifiers = new JSONArray();
            if (!excludeTabObs) {
                identifiers.put("tab_" + String.valueOf(specimen_id));
                identifiers.put("obs_" + String.valueOf(specimen_id));
                identifiers.put("tab_" + String.valueOf(specimen_id) + "_%");
                identifiers.put("obs_" + String.valueOf(specimen_id) + "_%");
            }
            identifiers.put(herbar_number);
            identifiers.put(herbar_number + "_%");
            identifiers.put(herbar_number + "A");
            identifiers.put(herbar_number + "B");

            // Finally list all resources using the identifier
            return this.listResources(identifiers, false);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Force import of a single identifier
     *
     * @param params
     */
    public String x_forceImport(JSONArray params) throws Exception {
        return this.forceImport(params.getString(0));
    }

    /**
     * Force import of a passed identifier
     *
     * @param identifier Identifier to force the import for
     */
    private String forceImport(String identifier) throws Exception {
        try {
            // Check import directory for identifier
            HashMap<String, String> importContent = Utilities.listDirectory(m_properties.getProperty("ImageServer.importDirectory"));
            String filePath = null;

            Iterator<Map.Entry<String, String>> icIt = importContent.entrySet().iterator();
            while (icIt.hasNext()) {
                Map.Entry<String, String> currEntry = icIt.next();

                // Check if this entry equals our identifier
                if (currEntry.getKey().equals(identifier)) {
                    filePath = currEntry.getValue();
                    break;
                }
            }

            // Check if we found a file
            if (filePath == null) {
                throw new Exception("No file with identifier '" + identifier + "' found. Please check the import directory.");
            }

            // Insert into import queue
            PreparedStatement iqInsert = m_conn.prepareStatement("INSERT INTO `import_queue` ( `identifier`, `filePath`, `force` ) values ( ?, ?, 1)");
            iqInsert.setString(1, identifier);
            iqInsert.setString(2, filePath);
            iqInsert.executeUpdate();
            iqInsert.close();

            // Try to start an import thread
            /*if( this.x_importImages() < 0 ) {
                throw new Exception( "Image added to queue, but unable to start import thread. Maybe it's already running?" );
            }*/
            // Everything went fine
            return "'" + identifier + "' added for import.";
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();

            throw new Exception(e.getMessage());
        }
    }

    /**
     * Change public status of a resource
     *
     * @param params
     */
    public String x_setPublic(JSONArray params) throws Exception {
        if(Utilities.isLegacy(m_properties)) {
            throw new Exception("Servlet is in legacy mode, cannot set public status");
        }
        return this.setPublic(params.getString(0), params.getBoolean(1));
    }

    /**
     * Change public status of a resource
     *
     * @param identifier Identifier of resource
     * @param bPublic true / false if resource should be public
     * @return
     */
    protected String setPublic(String identifier, boolean bPublic) throws Exception {
        try {
            // Update database with resource properties
            PreparedStatement updatePublicStat = m_conn.prepareStatement("UPDATE `resources` SET `public` = ? WHERE `identifier` = ?");
            updatePublicStat.setInt(1, (bPublic) ? 1 : 0);
            updatePublicStat.setString(2, identifier);
            updatePublicStat.executeUpdate();
            updatePublicStat.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();

            throw new Exception(e.getMessage());
        }

        return identifier;
    }

    /**
     * PUBLIC FUNCTIONS END
     */
    /**
     * Rescan the djatoka images directory and update the database (warning: may take long)
     */
    private int rescanDjatokaImagesDirectory() throws Exception {
        try {
            HashMap<String, String> dirContent = Utilities.listDirectory(m_properties.getProperty("ImageServer.resourcesDirectory"));

            // Cleanup old entries
            Statement stat = m_conn.createStatement();
            stat.execute("DELETE FROM `resources`");
            stat.close();

            // Now iterate through new list and add it to the database
            Iterator<Map.Entry<String, String>> dcIt = dirContent.entrySet().iterator();
            PreparedStatement prepStat = m_conn.prepareStatement("INSERT into `resources` values (?, ?);");
            while (dcIt.hasNext()) {
                Map.Entry<String, String> currEntry = dcIt.next();

                prepStat.setString(1, currEntry.getKey());
                prepStat.setString(2, currEntry.getValue());
                prepStat.addBatch();
            }

            // Execute the insert
            prepStat.executeBatch();
            prepStat.close();

            // Everything went fine, report back
            return 1;
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Internal utility functions, only for upgrading purpose
     */
    /**
     * Refresh the image metadata for all files which do not have the lastModified or size attribute set yet
     *
     * @deprecated Does not work at the current stage, needs modification to take relative archive path into account
     */
    private void refreshImageMetadata() {
        try {
            // Find all files which do not have lastModified or size set
            Statement stat = m_conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT `ar_id`, `imageFile` FROM `archive_resources` WHERE `lastModified` = 0 OR `size` = 0");

            // Cycle through files and update them
            while (rs.next()) {
                // Fetch file-database properties
                int ar_id = rs.getInt("ar_id");
                String imageFile = rs.getString("imageFile");

                // Construct new file handler
                File image = new File(imageFile);

                // Update database with image properties
                PreparedStatement refreshStat = m_conn.prepareStatement("UPDATE `archive_resources` SET `lastModified` = ?, `size` = ? WHERE `ar_id` = ?");
                refreshStat.setLong(1, image.lastModified() / 1000);
                refreshStat.setLong(2, image.length());
                refreshStat.setInt(3, ar_id);
                refreshStat.executeUpdate();
                refreshStat.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
