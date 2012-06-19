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
import java.sql.*;
import java.util.*;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.crypto.dsig.TransformException;
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
    
    private ImageImportThread m_importThread = null;
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
            Class.forName("org.sqlite.JDBC");
            m_conn = DriverManager.getConnection("jdbc:sqlite:" + m_properties.getProperty("ImageServer.database") );
            m_conn.setAutoCommit(true);
            
            // Check if database is already initialized
            Statement stat = m_conn.createStatement();
            // Check for resources table
            ResultSet rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `resources` ( `r_id` INTEGER CONSTRAINT `r_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT CONSTRAINT `identifier_unique` UNIQUE ON CONFLICT FAIL, `imageFile` TEXT )" );
            }
            rs.close();
            stat.close();
            // Check for import-log table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_logs' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `import_logs` ( `il_id` INTEGER CONSTRAINT `il_id_pk` PRIMARY KEY AUTOINCREMENT, `it_id` INTEGER, `logtime` INTEGER DEFAULT 0, `identifier` TEXT, `message` TEXT )" );
            }
            rs.close();
            stat.close();
            // Check for thread table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_threads' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate("CREATE table `import_threads` ( `it_id` INTEGER CONSTRAINT `it_id_pk` PRIMARY KEY AUTOINCREMENT, `thread_id` INTEGER, `starttime` INTEGER DEFAULT 0, `endtime` INTEGER DEFAULT 0 )" );
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
    public void x_importImages() {
        // Check if thread is already running
        if( m_importThread == null ) {
            try {
                m_importThread = new ImageImportThread();

                PreparedStatement prep = m_conn.prepareStatement( "INSERT INTO `import_threads` ( `thread_id`, `starttime` ) values ( ?, ? )" );
                prep.setLong(1, m_importThread.getId());
                prep.setLong(2, System.currentTimeMillis() / 1000);
                prep.executeUpdate();
                
                // Fetch auto-increment value and assign the unique number to our thread
                ResultSet it_id_result = prep.getGeneratedKeys();
                if( it_id_result.next() ) {
                    m_importThread.it_id = it_id_result.getInt(1);
                    m_importThread.start();
                    
                    m_response.element( "result", m_importThread.it_id );
                }
                // If we do not have any auto-increment value, something went terrible wrong
                else {
                    m_response.element( "result", "" );
                    m_response.element( "error", "Thread log insert failed - can't start!" );
                }

                // Free up statement
                it_id_result.close();
                prep.close();
            }
            // Something went wrong during thread startup
            catch( Exception e ) {
                m_response.element( "result", "" );
                m_response.element( "error", "Error while trying to start thread: " + e.getMessage() );
            }
        }
        else {
            m_response.element( "result", "" );
            m_response.element( "error", "Thread already running!" );
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
     * List all import threads
     */
    public void x_listImportThreads( JSONArray params ) {
        listImportThreads( params.getInt(0) );
    }
    
    /**
     * List all import threads
     * @param cutoff_date threads older than cutoff_date wont be returned
     */
    private void listImportThreads( int cutoff_date ) {
        try {
            JSONObject threads = new JSONObject();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `it_id`, `starttime` FROM `import_threads` WHERE `starttime` >= ? ORDER BY `thread_id`");
            stat.setString(1, String.valueOf(cutoff_date) );
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                threads.put( rs.getString("it_id"), rs.getString("starttime") );
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
     * @param thread_id 
     */
    private void listImportLogs( int it_id ) {
        try {
            JSONArray logs = new JSONArray();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `logtime`, `identifier`, `message` FROM `import_logs` WHERE `it_id` = ? ORDER BY `logtime` ASC");
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
    
    private void exportImages( String p_exportPath, JSONArray p_identifier ) {
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
                m_exportThread = new ExportThread(exportPath + p_exportPath, p_identifier, m_conn);
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
     * PUBLIC FUNCTIONS END
     */
    
    /**
     * Thread callback which notifies the servlet that the import is finished
     */
    private void importImagesFinished() {
        try {
            PreparedStatement prep = m_conn.prepareStatement("UPDATE `import_threads` set `endtime` = ? WHERE `it_id` = ?");
            prep.setLong(1, System.currentTimeMillis() / 1000);
            prep.setLong(2, m_importThread.it_id);
            prep.executeUpdate();
            prep.close();
        }
        catch( Exception e ) {
            System.err.println( "Error while finishing thread:" );
            e.printStackTrace();
        }
        
        m_importThread = null;
    }
    
    /**
     * Import thread which imports newly added images
     */
    private class ImageImportThread extends Thread {
        public int it_id = 0;
                
        /**
         * Import images waiting in import directory
         */
        @Override
        public void run() {
            try {
                int iqCount = 0;    // Import-Queue count
                
                // Disable auto-commit during imports
                m_conn.setAutoCommit(false);
                
                // Check if there are items waiting in the queue (e.g. due to a crash)...
                PreparedStatement queueStat = m_conn.prepareStatement("SELECT count(*) FROM `import_queue`");
                ResultSet rs = queueStat.executeQuery();
                if( rs.next() ) iqCount = rs.getInt(1);
                rs.close();
                queueStat.close();
                
                // If no items are waiting, fetch a list of fresh entries from the file-system
                if( iqCount <= 0 ) {
                    // Get a list of images to import
                    HashMap<String,String> importContent = listDirectory(m_properties.getProperty("ImageServer.importDirectory"));

                    // Cache list in database table
                    queueStat = m_conn.prepareStatement("INSERT INTO `import_queue` (`identifier`, `filePath`) values (?, ?)");
                    Iterator<Map.Entry<String,String>> icIt = importContent.entrySet().iterator();
                    // Cycle through results and store them in the database
                    while( icIt.hasNext() ) {
                        Map.Entry<String,String> entry = icIt.next();
                        String identifier = entry.getKey();
                        String inputName = entry.getValue();
                        
                        // Assign properties to statement
                        queueStat.setString(1, identifier);
                        queueStat.setString(2, inputName);
                        queueStat.executeUpdate();
                    }
                    // Finally execute & commit the queue-list
                    queueStat.close();
                    m_conn.commit();
                }

                // Prepare statement for identifier check
                PreparedStatement existsStat = m_conn.prepareStatement( "SELECT `identifier` FROM `archive_resources` WHERE `identifier` = ? AND `obsolete` = 0" );
                // Find all entries in queue
                queueStat = m_conn.prepareStatement("SELECT * FROM `import_queue`");
                ResultSet statRs = queueStat.executeQuery();
                // Fetch entries into temporary memory
                HashMap<String, HashMap<String, Object>> importQueue = new HashMap<String, HashMap<String, Object>>();
                while( statRs.next() ) {
                    HashMap<String, Object> queueObj = new HashMap<String, Object>();
                    queueObj.put( "filePath", statRs.getString("filePath") );
                    queueObj.put( "force", statRs.getInt("force") );
                    
                    importQueue.put(statRs.getString("identifier"), queueObj);
                }
                queueStat.close();
                statRs.close();
                
                // Iterate over entries list and process them
                Iterator<Map.Entry<String,HashMap<String, Object>>> iqIt = importQueue.entrySet().iterator();
                while( iqIt.hasNext() ) {
                    // Fetch important information from queue-list
                    Map.Entry<String,HashMap<String, Object>> entry = iqIt.next();
                    String identifier = entry.getKey();
                    HashMap<String, Object> queueObj = entry.getValue();
                    String inputName = (String) queueObj.get("filePath");
                    int force = (Integer) queueObj.get("force");
                    
                    // Start working on entry
                    try {
                        // Check if the identifier already exists
                        existsStat.setString(1, identifier);
                        rs = existsStat.executeQuery();
                        boolean status = rs.next();
                        rs.close();
                        // Allow forced adds anyway
                        if( !status || force == 1 ) {
                            // Create output file-name & path
                            String temporaryName = m_properties.getProperty("ImageServer.tempDirectory") + identifier + ".tif";
                            
                            // Check if we want to watermark the image
                            if( !m_properties.getProperty("ImageServer.watermark").isEmpty() ) {
                                // Watermark the image
                                // Note: [0] for the input-file in order to avoid conflicts with multi-page tiffs
                                Process watermarkProc = new ProcessBuilder( m_properties.getProperty("ImageServer.imComposite"), "-quiet", "-gravity", "SouthEast", m_properties.getProperty("ImageServer.watermark"), inputName + "[0]", temporaryName ).start();
                                watermarkProc.waitFor();
                                watermarkProc.getErrorStream().close();
                                watermarkProc.getInputStream().close();
                                watermarkProc.getOutputStream().close();
                                watermarkProc.destroy();
                            }
                            else {
                                // No temporary file, so use input directly
                                temporaryName = inputName;
                            }

                            // Check if the watermarking was successfull
                            File temporaryFile = new File(temporaryName);
                            if( temporaryFile.exists() ) {
                                // Create output directory for djatoka
                                File inputFile = new File(inputName);
                                String outputName = Utilities.createDirectory(m_properties.getProperty("ImageServer.resourcesDirectory"), inputFile.lastModified()) + identifier + ".jp2";
                                
                                // Convert new image
                                Process compressProc = new ProcessBuilder( m_properties.getProperty("ImageServer.dCompress"), "-i", temporaryName, "-o", outputName ).start();
                                compressProc.waitFor();
                                compressProc.getErrorStream().close();
                                compressProc.getInputStream().close();
                                compressProc.getOutputStream().close();
                                compressProc.destroy();
                                
                                // Remove temporary file
                                temporaryFile.delete();

                                // Check if image conversion was successfull
                                File outputFile = new File(outputName);
                                if( outputFile.exists() ) {
                                    // Check if an old entry already existed
                                    if( status ) {
                                        // Remove old resources entry
                                        PreparedStatement resDelStat = m_conn.prepareCall("DELETE FROM `resources` WHERE `identifier` = ?");
                                        resDelStat.setString(0, identifier);
                                        resDelStat.executeUpdate();
                                        resDelStat.close();
                                    }
                                    
                                    // Insert id into database
                                    PreparedStatement resourcesStat = m_conn.prepareStatement( "INSERT INTO `resources` ( `identifier`, `imageFile` ) values (?, ?)" );
                                    resourcesStat.setString(1, identifier);
                                    resourcesStat.setString(2, outputName );
                                    resourcesStat.executeUpdate();
                                    resourcesStat.close();
                                    
                                    // Create archive directory
                                    File archiveFile = new File( Utilities.createDirectory(m_properties.getProperty("ImageServer.archiveDirectory"), inputFile.lastModified() ) + inputFile.getName() );

                                    // Check if destination does not exist
                                    if( !archiveFile.exists() && archiveFile.getParentFile().canWrite() ) {
                                        // Copy the file into the archive
                                        int exitCode = Utilities.copyFile(inputFile.getPath(), archiveFile.getPath());
                                        if( exitCode == 0 ) {
                                            // Compare input and archive file
                                            if( inputFile.length() == archiveFile.length() ) {
                                                // Check if an old entry already existed
                                                if( status ) {
                                                    // Mark old entry as obsolete
                                                    PreparedStatement archiveUpdateStat = m_conn.prepareStatement("UPDATE `archive_resources` SET `obsolete` = 1 WHERE `identifier` = ?");
                                                    archiveUpdateStat.setString(0, identifier);
                                                    archiveUpdateStat.executeUpdate();
                                                    archiveUpdateStat.close();
                                                }
                                                
                                                // Update archive resources list
                                                PreparedStatement archiveStat = m_conn.prepareStatement( "INSERT INTO `archive_resources` ( `identifier`, `imageFile`, `lastModified`, `size`, `it_id` ) values (?, ?, ?, ?, ?)" );
                                                archiveStat.setString(1, identifier);
                                                archiveStat.setString(2, archiveFile.getAbsolutePath());
                                                archiveStat.setLong(3, inputFile.lastModified() / 1000);
                                                archiveStat.setLong(4, inputFile.length());
                                                archiveStat.setInt(5, it_id);
                                                archiveStat.executeUpdate();
                                                archiveStat.close();
                                                
                                                // Finally make sure our new data is written to disk
                                                m_conn.commit();

                                                // Remove input file
                                                inputFile.delete();
                                            }
                                            else {
                                                throw new TransformException( "Validity check of file failed [" + inputFile.getPath() + " => " + archiveFile.getPath() + "]" );
                                            }
                                        }
                                        else {
                                            throw new TransformException( "Unable to move file into archive [" + inputFile.getPath() + " => " + archiveFile.getPath() + "] - exit-code [" + exitCode + "]" );
                                        }
                                    }
                                    else {
                                        throw new TransformException( "File exists or cannot write archive path [" + archiveFile.getPath() + "]" );
                                    }
                                }
                                else {
                                    throw new TransformException( "Writing file for Djatoka failed [" + outputFile.getPath() + "]" );
                                }
                            }
                            else {
                                throw new TransformException( "Watermarking image failed [" + temporaryFile.getPath() + "]" );
                            }
                        }
                        else {
                            throw new TransformException( "Import of [" + inputName + "] failed. Identifier already exists [" + identifier + "]" );
                        }
                    }
                    catch( Exception e ) {
                        // Something went wrong, rollback
                        m_conn.rollback();
                        
                        // Log this issue
                        PreparedStatement logStat = m_conn.prepareStatement( "INSERT INTO `import_logs` ( `it_id`, `logtime`, `identifier`, `message` ) values(?, ?, ?, ?)" );
                        logStat.setInt(1,it_id);
                        logStat.setString(2, String.valueOf(System.currentTimeMillis() / 1000) );
                        logStat.setString(3, identifier);
                        logStat.setString(4, e.getMessage() );
                        logStat.executeUpdate();
                        logStat.close();
                        
                        // Commit the log message
                        m_conn.commit();
                    }
                    // Remove item from import queue
                    // This is done in any case since else it will stay in the queue and block it forever...
                    queueStat = m_conn.prepareStatement("DELETE FROM `import_queue` WHERE `identifier` = ?");
                    queueStat.setString(1, identifier);
                    queueStat.executeUpdate();
                    queueStat.close();
                    m_conn.commit();
                }
                // Release prepared statements
                existsStat.close();
                // Enable auto-commit
                m_conn.setAutoCommit(true);
            }
            catch(SQLException e ) {
                System.err.println( "[" + e.getErrorCode() + "] " + e.getSQLState() + " [" + e.getMessage() + "]");
                e.printStackTrace();
            }
            catch( Exception e ) {
                System.err.println( e.toString() );
                e.printStackTrace();
            }
            
            // Notify parent that we are done
            importImagesFinished();
        }
    }
    
    /**
     * Rescan the djatoka images directory and update the database (warning: may take long)
     */
    private void rescanDjatokaImagesDirectory() {
        try {
            HashMap<String,String> dirContent = listDirectory(m_properties.getProperty("ImageServer.resourcesDirectory"));

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
     * Internal helper function for listing the content of a directory (+ creating IDs for the entries)
     * @param sourceDir Directory to list
     * @return HashMap of identifier => path entries
     */
    private HashMap<String,String> listDirectory( String sourceDir ) {
        HashMap<String,String> dirContent = new HashMap<String,String>();
        
        File dir = new File( sourceDir );
        File dirEntries[] = dir.listFiles();
        
        for(int i = 0; i < dirEntries.length; i++ ) {
            File dirEntry = dirEntries[i];
            if( dirEntry.isDirectory() ) {
                dirContent.putAll( listDirectory(dirEntry.getAbsolutePath()) );
            }
            else {
                dirContent.put(dirEntry.getName().substring(0, dirEntry.getName().lastIndexOf(".")), dirEntry.getAbsolutePath());
            }
        }
        
        return dirContent;
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
