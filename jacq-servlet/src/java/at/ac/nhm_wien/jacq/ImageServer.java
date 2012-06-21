package at.ac.nhm_wien.jacq;

/*
JACQ
Copyright (C) 2011-2012 Naturhistorisches Museum Wien

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

import java.io.*;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/**
 *
 * @author wkoller
 */
public class ImageServer extends HttpServlet {
    /**
     * Used by all classes to access the configuration settings
     */
    public static Properties m_properties = new Properties();

    private String m_requestId = "";
    private JSONArray m_requestParams = null;
    private JSONObject m_response = null;
    private Connection m_conn = null;
    
    private ImportThread m_importThread = null;
    private ExportThread m_exportThread = null;
    
    /**
     * Initialize Servlet
     * @throws ServletException 
     */
    @Override
    public void init() throws ServletException {
        try {
            // Load properties
            m_properties.load(new FileInputStream(getServletContext().getRealPath( "/WEB-INF/"+ this.getClass().getSimpleName() + ".properties" )));
            // Establish "connection" to SQLite database
            m_conn = Utilities.getConnection();
            
            // Check if database is already initialized
            Statement stat = m_conn.createStatement();
            // Check for resources table
            ResultSet rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `resources` ( `r_id` INTEGER CONSTRAINT `r_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT CONSTRAINT `identifier_unique` UNIQUE ON CONFLICT FAIL, `imageFile` TEXT )" );
            }
            rs.close();
            stat.close();
            // Check for archive resources table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'archive_resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `archive_resources` ( `ar_id` INTEGER CONSTRAINT `ar_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT, `imageFile` TEXT, `lastModified` INTEGER DEFAULT 0, `size` INTEGER DEFAULT 0, `obsolete` INTEGER DEFAULT 0, `it_id` INTEGER )" );
            }
            rs.close();
            stat.close();
            // Check for import queue table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_queue' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `import_queue` ( `iq_id` INTEGER CONSTRAINT `iq_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT, `filePath` TEXT, `force` INTEGER DEFAULT 0 )" );
            }
            rs.close();
            stat.close();
            // Check for export queue table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'export_queue' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `export_queue` ( `eq_id` INTEGER CONSTRAINT `eq_id_pk` PRIMARY KEY AUTOINCREMENT, `archiveFilePath` TEXT, `exportFilePath` TEXT )" );
            }
            rs.close();
            stat.close();
            // Check for threads table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'threads' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `threads` ( `t_id` INTEGER CONSTRAINT `t_id_pk` PRIMARY KEY AUTOINCREMENT, `thread_id` INTEGER, `starttime` INTEGER DEFAULT 0, `endtime` INTEGER DEFAULT 0, `type` INTEGER DEFAULT 0 )");
            }
            rs.close();
            stat.close();
            // Check for threads logging table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'thread_logs' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `thread_logs` ( `tl_id` INTEGER CONSTRAINT `tl_id_pk` PRIMARY KEY AUTOINCREMENT, `t_id` INTEGER, `logtime` INTEGER DEFAULT 0, `identifier` TEXT DEFAULT '', `message` TEXT )");
            }
            rs.close();
            stat.close();
        }
        catch( Exception e ) {
            System.err.println( e.getMessage() );
        }
    }

    @Override
    public void destroy() {
        try {
            m_conn.close();
        }
        catch( Exception e ) {
            System.err.println( "Unable to close connection to SQLite: " + e.getMessage() );
        }
        
        super.destroy();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if( req.getParameter("request") != null ) {
            resp.getOutputStream().print(handleRequest(JSONObject.fromObject(req.getParameter("request"))));
        }
        // Quite hacky way to support Mootools interpretation of a JSON-RPC call
        else {
            JSONObject reqObject = new JSONObject();
            JSONArray params = new JSONArray();
            
            Map<String, String[]> parameters = req.getParameterMap();
            
            Iterator<Map.Entry<String, String[]>> paramsIt = parameters.entrySet().iterator();
            while(paramsIt.hasNext()) {
                Map.Entry<String,String[]> currEntry = paramsIt.next();
                
                // Id & Method are handled special
                if( currEntry.getKey().equals( "id" ) || currEntry.getKey().equals( "method" ) ) {
                    reqObject.element( currEntry.getKey(), currEntry.getValue()[0] );
                }
                else {
                    // Check if we have passed an array as parameter
                    if( currEntry.getValue().length > 1 ) {
                        params.element( JSONArray.fromObject(currEntry.getValue()) );
                    }
                    else {
                        params.element( currEntry.getValue()[0] );
                    }
                }
            }
            
            // Finally add the parameter as well
            reqObject.element("params", params);
            
            // Run the query
            resp.getOutputStream().print(handleRequest(reqObject));
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String inputString = "";
        BufferedReader in = new BufferedReader(new InputStreamReader(req.getInputStream()));
        for( String buffer; (buffer = in.readLine()) != null; inputString += buffer + "\n" );
        
        resp.getOutputStream().print(handleRequest(JSONObject.fromObject(inputString)));
    }
    
    /**
     * Handle an incoming request (already parsed as JSON-RPC request)
     * @param reqObject Contains the request object
     * @return The output which should be sent to the client
     */
    protected String handleRequest(JSONObject reqObject) {
        m_response = new JSONObject();
        String methodName = "";
        if( !reqObject.isNullObject() ) {
            try {
                // Fetch passed parameters
                m_requestId = reqObject.getString("id");
                m_response.put("id", m_requestId);
                m_requestParams = reqObject.getJSONArray("params");
                methodName = reqObject.getString("method");  // Prefix method with 'x_' to preven arbitrary executions

                // Check if we have at least one key
                if( m_requestParams.isEmpty() ) {
                    throw new Exception( "No key passed" );
                }
                
                // First parameter always MUST be the authentication key
                String requestKey = m_requestParams.getString(0);
                m_requestParams.remove(0);
                
                // Check if requestKey is valid
                if( !requestKey.equals(m_properties.getProperty("ImageServer.key")) ) {
                    throw new Exception( "Invalid key passed" );
                }

                // Find correct method to call
                Method callMethod = null;
                Method[] methods = this.getClass().getMethods();
                for( Method method : methods ) {
                    if( method.getName().equals("x_" + methodName) ) {
                        // Check if the parameters do fit
                        Class[] parameterTypes = method.getParameterTypes();
                        
                        // Check if we have parameters
                        if( m_requestParams.isEmpty() && parameterTypes.length == 0 ) {
                            callMethod = method;
                            callMethod.invoke(this);
                            break;
                        }
                        else if( m_requestParams.size() > 0 && parameterTypes.length > 0 && parameterTypes[0] == JSONArray.class ) {
                            callMethod = method;
                            callMethod.invoke(this, m_requestParams);
                            break;
                        }
                    }
                }
                
                // Check if we found a method at all
                if( callMethod == null ) {
                    throw new Exception( "Method not found" );
                }
            }
            catch(Exception e ) {
                m_response.element("result", "");
                m_response.element("error", "Unable to call requested method ('" + methodName + "'): " + e.getMessage() + " / " + e.toString() );
            }
        }

        return m_response.toString();
    }
    
    /**
     * PUBLIC FUNCTIONS START
     */
    /**
     * Starts a thread for importing new images
     */
    public int x_importImages() {
        // Check if importthread is still active
        if( m_importThread != null && !m_importThread.isAlive() ) {
            m_importThread = null;
        }
        
        // Check if thread is already running
        if( m_importThread == null ) {
            try {
                // Start new import thread
                m_importThread = new ImportThread();
                m_importThread.start();

                // Return thread id
                m_response.element( "result", m_importThread.getThread_id() );
                return m_importThread.getThread_id();
            }
            // Something went wrong during thread startup
            catch( Exception e ) {
                m_response.element( "result", "" );
                m_response.element( "error", "Error while trying to start thread: " + e.getMessage() );
                
                return -1;
            }
        }
        else {
            m_response.element( "result", "" );
            m_response.element( "error", "Thread already running!" );
            
            return -2;
        }
    }
    
    /**
     * Wheter if we want the obsoletes can be passed as first parameter
     * @param params 
     */
    public void x_listArchiveImages(JSONArray params) {
        this.listArchiveImages( (params.getBoolean(0)) ? 1 : 0 );
    }
    
    /**
     * Calling listArchiveImages without parameters is allowed as well
     */
    public void x_listArchiveImages() {
        this.listArchiveImages(0);
    }
    
    /**
     * List all images currently stored in the archive
     * @param p_obsolete return obsolete entries
     */
    private void listArchiveImages( int p_obsolete ) {
        try {
            JSONArray resources = new JSONArray();

            PreparedStatement prepStat = m_conn.prepareStatement("SELECT `identifier` FROM `archive_resources` WHERE `obsolete` = ? ORDER BY `identifier`");
            prepStat.setInt(1, p_obsolete);
            ResultSet rs = prepStat.executeQuery();
            
            while(rs.next()) {
                resources.add( rs.getString("identifier") );
            }
            rs.close();
            prepStat.close();
            
            m_response.put("result", resources);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * List all images currently stored in the archive
     */
    public void x_listDjatokaImages() {
        
        try {
            JSONArray resources = new JSONArray();
            
            Statement stat = m_conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT `identifier` FROM `resources` ORDER BY `identifier`");
            while(rs.next()) {
                resources.add( rs.getString("identifier") );
            }
            rs.close();
            stat.close();
            
            m_response.put("result", resources);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * Get a list of threads (after a certain date, optionally filtered by type)
     */
    public void x_listThreads( JSONArray params ) {
        if( params.size() > 1 ) {
            listThreads(params.getInt(0), params.getInt(1));
        }
        // By default do not filter by thread type
        else {
            listThreads(params.getInt(0), 0);
        }
    }
    
    /**
     * List all import threads
     * @param cutoff_date threads older than cutoff_date wont be returned
     * @param type limit returned threads to a certain type
     */
    private void listThreads( int cutoff_date, int type ) {
        try {
            JSONObject threads = new JSONObject();

            // Prepare statement
            PreparedStatement stat = null;
            // Check if we have to filter by type
            if( type > 0 ) {
                stat = m_conn.prepareStatement("SELECT `t_id`, `starttime`, `endtime`, `type` FROM `threads` WHERE `starttime` >= ? AND `type` = ? ORDER BY `thread_id`");
                stat.setInt(2, type);
            }
            else {
                stat = m_conn.prepareStatement("SELECT `t_id`, `starttime`, `endtime`, `type` FROM `threads` WHERE `starttime` >= ? ORDER BY `thread_id`");
            }
            stat.setInt(1, cutoff_date);
            
            // Fetch all fitting threads
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                // Combine thread related information in an object
                JSONObject threadInfo = new JSONObject();
                threadInfo.put("starttime", rs.getInt("starttime"));
                threadInfo.put("endtime", rs.getInt("endtime"));
                threadInfo.put("type", rs.getInt("type"));
                
                // Add thread info to stack
                threads.put( rs.getString("t_id"), threadInfo );
            }
            rs.close();
            stat.close();
            
            m_response.put("result", threads);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * Returns a list of log messages for a given thread-id
     */
    public void x_listImportLogs( JSONArray params ) {
        listImportLogs( params.getInt(0) );
    }
    
    /**
     * Returns a list of log messages for a given thread-id
     * @param it_id 
     */
    private void listImportLogs( int it_id ) {
        try {
            JSONArray logs = new JSONArray();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `logtime`, `identifier`, `message` FROM `thread_logs` WHERE `t_id` = ? ORDER BY `logtime` ASC");
            stat.setString(1, String.valueOf(it_id) );
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                JSONObject logObj = new JSONObject();
                logObj.put("logtime", rs.getString("logtime"));
                logObj.put("identifier", rs.getString("identifier"));
                logObj.put("message", rs.getString("message"));
                logs.add( logObj );
            }
            rs.close();
            stat.close();
            
            m_response.put("result", logs);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * Start exporting images (callable function)
     */
    public void x_exportImages( JSONArray params ) {
        exportImages("", params.getJSONArray(0));
    }
    
    /**
     * Start exporting images
     * @param p_exportPath relative path inside the exportDirectory property
     * @param p_identifier list of identifiers to export
     */
    private void exportImages( String p_exportPath, JSONArray p_identifiers ) {
        // Check if thread is still alive
        if( m_exportThread != null && !m_exportThread.isAlive() ) {
            m_exportThread = null;
        }
        
        // Check for export thread
        if( m_exportThread == null ) {
            try {
                // Construct export path
                String exportPath = m_properties.getProperty("ImageServer.exportDirectory");
                
                // Replace any ../ with an empty string
                // Note: this regular expression does not treat ../ very cleanly,
                // however we only want to prevent overwriting anything outside the base-path
                p_exportPath = p_exportPath.replaceAll("\\.\\./", "");
                
                // Check for trailing slash
                if( !exportPath.endsWith("/") ) {
                    exportPath += "/";
                }
                
                // Create new export thread
                m_exportThread = new ExportThread(exportPath + p_exportPath, p_identifiers);
                m_exportThread.start();

                // We finished successfully if we reach here
                m_response.put("result", m_exportThread.getThread_id());
            }
            catch( Exception e ) {
                m_response.put("error", e.getMessage() );
                m_response.put("result", "");
            }
        }
        // ... thread already running
        else {
            m_response.put("error", "Export thread already running" );
            m_response.put("result", "");
        }
    }
    
    /**
     * Returns a list of file identifiers for a given specimen
     */
    public void x_listSpecimenImages( JSONArray params ) {
        listSpecimenImages( params.getInt(0), params.getString(1) );
    }
    
    /**
     * Returns a list of file identifiers for a given specimen
     * @param specimen_id Specimen ID
     * @param herbar_number Herbarnumber of specimen
     */
    private void listSpecimenImages( int specimen_id, String herbar_number ) {
        try {
            JSONArray images = new JSONArray();
            
            // Try to find all possible variants of this image
            PreparedStatement stat = m_conn.prepareStatement("SELECT `identifier` FROM `resources` WHERE `identifier` = ? OR `identifier` = ? OR `identifier` = ? OR `identifier` LIKE ? ESCAPE '\\' ORDER BY `identifier` ASC");
            stat.setString(1, "tab_" + String.valueOf(specimen_id));
            stat.setString(2, "obs_" + String.valueOf(specimen_id));
            stat.setString(3, herbar_number );
            stat.setString(4, herbar_number + "\\_%" );
            
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                images.add( rs.getString("identifier") );
            }
            rs.close();
            stat.close();
            
            m_response.put("result", images);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * Force import of a single identifier
     * @param params 
     */
    public void x_forceImport(JSONArray params) {
        this.forceImport(params.getString(0));
    }
    
    /**
     * Force import of a passed identifier
     * @param identifier Identifier to force the import for
     */
    private void forceImport(String identifier) {
        try {
            // Check import directory for identifier
            HashMap<String,String> importContent = Utilities.listDirectory(m_properties.getProperty("ImageServer.importDirectory"));
            String filePath = null;
            
            Iterator<Map.Entry<String,String>> icIt = importContent.entrySet().iterator();
            while(icIt.hasNext()) {
                Map.Entry<String,String> currEntry = icIt.next();
                
                // Check if this entry equals our identifier
                if( currEntry.getKey().equals(identifier) ) {
                    filePath = currEntry.getValue();
                    break;
                }
            }
            
            // Check if we found a file
            if( filePath == null ) {
                throw new Exception( "No file with identifier '" + identifier + "' found. Please check the import directory." );
            }
            
            // Insert into import queue
            PreparedStatement iqInsert = m_conn.prepareStatement("INSERT INTO `import_queue` ( `identifier`, `filePath`, `force` ) values ( ?, ?, 1)");
            iqInsert.setString(1, identifier);
            iqInsert.setString(2, filePath);
            iqInsert.executeUpdate();
            
            // Try to start an import thread
            if( this.x_importImages() < 0 ) {
                throw new Exception( "Image added to queue, but unable to start import thread. Maybe it's already running?" );
            }
            
            
            // Everything went fine
            m_response.put("result", "'" + identifier + "' added for import.");
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }

    /**
     * PUBLIC FUNCTIONS END
     */
    /**
     * Rescan the djatoka images directory and update the database (warning: may take long)
     */
    private void rescanDjatokaImagesDirectory() {
        try {
            HashMap<String,String> dirContent = Utilities.listDirectory(m_properties.getProperty("ImageServer.resourcesDirectory"));

            // Cleanup old entries
            Statement stat = m_conn.createStatement();
            stat.execute("DELETE FROM `resources`");
            stat.close();
            
            // Now iterate through new list and add it to the database
            Iterator<Map.Entry<String,String>> dcIt = dirContent.entrySet().iterator();
            PreparedStatement prepStat = m_conn.prepareStatement( "INSERT into `resources` values (?, ?);" );
            while(dcIt.hasNext()) {
                Map.Entry<String,String> currEntry = dcIt.next();
                
                prepStat.setString(1, currEntry.getKey());
                prepStat.setString(2, currEntry.getValue());
                prepStat.addBatch();
            }

            // Execute the insert
            prepStat.executeBatch();
            prepStat.close();

            // Everything went fine, report back
            m_response.element("result", "1");
        }
        catch( Exception e ) {
            m_response.element("result", "");
            m_response.element("error", e.getMessage());
        }
    }
    
    /**
     * Internal utility functions, only for upgrading purpose
     */
    
    /**
     * Refresh the image metadata for all files which do not have the lastModified or size attribute set yet
     */
    private void refreshImageMetadata() {
        try {
            // Find all files which do not have lastModified or size set
            Statement stat = m_conn.createStatement();
            ResultSet rs = stat.executeQuery( "SELECT `ar_id`, `imageFile` FROM `archive_resources` WHERE `lastModified` = 0 OR `size` = 0" );
            
            // Cycle through files and update them
            while( rs.next() ) {
                // Fetch file-database properties
                int ar_id = rs.getInt( "ar_id" );
                String imageFile = rs.getString( "imageFile" );
                
                // Construct new file handler
                File image = new File(imageFile);
                
                // Update database with image properties
                PreparedStatement refreshStat = m_conn.prepareStatement( "UPDATE `archive_resources` SET `lastModified` = ?, `size` = ? WHERE `ar_id` = ?" );
                refreshStat.setLong( 1, image.lastModified() / 1000 );
                refreshStat.setLong( 2, image.length() );
                refreshStat.setInt( 3, ar_id );
                refreshStat.executeUpdate();
                refreshStat.close();
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
    }
}
