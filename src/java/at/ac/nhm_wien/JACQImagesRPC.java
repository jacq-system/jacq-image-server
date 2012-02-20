package at.ac.nhm_wien;

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
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
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
public class JACQImagesRPC extends HttpServlet {
    private String m_requestId = "";
    private JSONArray m_requestParams = null;
    private JSONObject m_response = null;
    private Properties m_properties = new Properties();
    private Connection m_conn = null;
    
    private ImageImportThread m_importThread = null;
    
    @Override
    public void init() throws ServletException {
        try {
            // Load properties
            m_properties.load(new FileInputStream(getServletContext().getRealPath( "/WEB-INF/"+ getClass().getSimpleName() + ".properties" )));
            // Establish "connection" to SQLite database
            Class.forName("org.sqlite.JDBC");
            m_conn = DriverManager.getConnection("jdbc:sqlite:" + m_properties.getProperty("JACQImagesRPC.database") );
            m_conn.setAutoCommit(true);
            
            // Check if database is already initialized
            Statement stat = m_conn.createStatement();
            // Check for resources table
            ResultSet rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `resources` ( `r_id` INTEGER CONSTRAINT `r_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT CONSTRAINT `identifier_unique` UNIQUE ON CONFLICT FAIL, `imageFile` TEXT )" );
            }
            rs.close();
            stat.close();
            // Check for import-log table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_logs' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `import_logs` ( `il_id` INTEGER CONSTRAINT `il_id_pk` PRIMARY KEY AUTOINCREMENT, `it_id` INTEGER, `logtime` INTEGER DEFAULT 0, `identifier` TEXT, `message` TEXT )" );
            }
            rs.close();
            stat.close();
            // Check for thread table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_threads' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `import_threads` ( `it_id` INTEGER CONSTRAINT `it_id_pk` PRIMARY KEY AUTOINCREMENT, `thread_id` INTEGER, `starttime` INTEGER DEFAULT 0, `endtime` INTEGER DEFAULT 0 )" );
            }
            rs.close();
            stat.close();
            // Check for archive resources table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'archive_resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `archive_resources` ( `ar_id` INTEGER CONSTRAINT `ar_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier` TEXT, `imageFile` TEXT )" );
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
        if( !reqObject.isNullObject() ) {
            m_requestId = reqObject.getString("id");
            m_requestParams = reqObject.getJSONArray("params");

            String methodName = reqObject.getString("method");
            m_response.put("id", m_requestId);
            try {
                if( m_requestParams.size() > 0 ) {
//                    Class[] paramsClass = new Class[m_requestParams.size()];
//                    String[] params = (String[]) m_requestParams.toArray(new String[1]);
//                    for( int i = 0; i < params.length; i++ ) {
//                        paramsClass[i] = params[i].getClass();
//                    }
//
//                    this.getClass().getMethod(methodName, paramsClass ).invoke(this, m_requestParams.toArray());
                    Class[] paramsClass = { JSONArray.class };
                    this.getClass().getMethod(methodName, paramsClass ).invoke(this, m_requestParams);
                }
                else {
                    this.getClass().getMethod(methodName ).invoke(this, m_requestParams.toArray());
                }
            }
            catch(Exception e ) {
                m_response.element("result", "");
                m_response.element("error", "Unable to call requested method: " + e.getMessage() + " / " + e.toString() );
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
    public void importImages() {
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
                    
                    m_response.element( "result", "1" );
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
     * List all images currently stored in the archive
     */
    public void listArchiveImages() {
        try {
            JSONArray resources = new JSONArray();
            
            Statement stat = m_conn.createStatement();
            ResultSet rs = stat.executeQuery("SELECT `identifier` FROM `archive_resources` ORDER BY `identifier`");
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
     * List all images currently stored in the archive
     */
    public void listDjatokaImages() {
        
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
    public void listImportThreads( JSONArray params ) {
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
    public void listImportLogs( JSONArray params ) {
        listImportLogs( params.getInt(0) );
    }
    
    /**
     * Returns a list of log messages for a given thread-id
     * @param thread_id 
     */
    private void listImportLogs( int it_id ) {
        try {
            JSONArray logs = new JSONArray();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `logtime`, `identifier`, `message` FROM `import_logs` WHERE `it_id` >= ? ORDER BY `logtime` ASC");
            stat.setString(1, String.valueOf(it_id) );
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                logs.add( "[" + rs.getString("logtime") + "] [" + rs.getString("identifier") + "] " + rs.getString("message") );
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
     * Returns a list of file identifiers for a given specimen
     */
    public void listSpecimenImages( JSONArray params ) {
        listSpecimenImages( params.getInt(1), params.getString(0) );
    }
    
    /**
     * Returns a list of file identifiers for a given specimen
     * @param specimen_id Specimen ID
     * @param herbar_number Herbarnumber of specimen
     */
    private void listSpecimenImages( int specimen_id, String herbar_number ) {
        try {
            JSONArray images = new JSONArray();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `identifier` FROM `resources` WHERE `identifier` LIKE ? OR `identifier` LIKE ? ORDER BY `identifier` ASC");
            stat.setString(1, "%" + String.valueOf(specimen_id) + "%" );
            stat.setString(2, herbar_number );
            
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
         * Small helper function which creates an output directory according to our formatting rules
         * @param p_baseDir Base dir for directory creation
         * @param p_modificationDate Modification time which is used for directory naming
         * @return String Returns the name of the output directory on success (with trailing slash)
         * @throws TransformException 
         */
        private String createDirectory( String p_baseDir, long p_modificationDate ) throws TransformException {
            File baseDir = new File(p_baseDir);
            if( baseDir.exists() && baseDir.isDirectory() && baseDir.canWrite() ) {
                // Create formatted output directory
                SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy/yyMMdd/");
                Date now =  new Date(p_modificationDate);
                File subDir = new File(baseDir, yearFormat.format(now));
                
                // Check if subDir already exists or try to create it
                if( subDir.exists() || subDir.mkdirs() ) {
                    return subDir.getPath() + "/";
                }
                else {
                    throw new TransformException( "Unable to access sub-dir [" + subDir.getPath() + "]" );
                }
            }
            else {
                throw new TransformException( "Unable to access base-dir [" + baseDir.getPath() + "]" );
            }
        }
        
        /**
         * Import images waiting in import directory
         */
        @Override
        public void run() {
            try {
                // Get a list of images to import
                HashMap<String,String> importContent = listDirectory(m_properties.getProperty("JACQImagesRPC.importDirectory"));

                Iterator<Map.Entry<String,String>> icIt = importContent.entrySet().iterator();
                PreparedStatement existsStat = m_conn.prepareStatement( "SELECT `identifier` FROM `archive_resources` WHERE `identifier` = ?" );
                while( icIt.hasNext() ) {
                    Map.Entry<String,String> entry = icIt.next();
                    String identifier = entry.getKey();
                    try {
                        existsStat.setString(1, identifier);

                        // Check if the identifier already exists
                        ResultSet rs = existsStat.executeQuery();
                        boolean status = rs.next();
                        rs.close();
                        if( !status ) {
                            // Create input and output file-names & paths
                            String inputName = entry.getValue();
                            String temporaryName = m_properties.getProperty("JACQImagesRPC.tempDirectory") + identifier + ".tif";
                            
                            // Check if we want to watermark the image
                            if( !m_properties.getProperty("JACQImagesRPC.watermark").isEmpty() ) {
                                // Watermark the image
                                // Note: [0] for the input-file in order to avoid conflicts with multi-page tiffs
                                Process watermarkProc = new ProcessBuilder( m_properties.getProperty("JACQImagesRPC.imComposite"), "-quiet", "-gravity", "SouthEast", m_properties.getProperty("JACQImagesRPC.watermark"), inputName + "[0]", temporaryName ).start();
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
                                String outputName = createDirectory(m_properties.getProperty("JACQImagesRPC.resourcesDirectory"), inputFile.lastModified()) + identifier + ".jp2";
                                
                                // Convert new image
                                Process compressProc = new ProcessBuilder( m_properties.getProperty("JACQImagesRPC.dCompress"), "-i", temporaryName, "-o", outputName ).start();
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
                                    // Insert id into database
                                    PreparedStatement resourcesStat = m_conn.prepareStatement( "INSERT INTO `resources` ( `identifier`, `imageFile` ) values (?, ?)" );
                                    resourcesStat.setString(1, identifier);
                                    resourcesStat.setString(2, outputName );
                                    resourcesStat.executeUpdate();
                                    resourcesStat.close();
                                    
                                    // Create archive directory
                                    File archiveFile = new File( createDirectory(m_properties.getProperty("JACQImagesRPC.archiveDirectory"), inputFile.lastModified() ) + inputFile.getName() );

                                    // Check if destination does not exist
                                    if( !archiveFile.exists() && archiveFile.getParentFile().canWrite() ) {
                                        // Copy the file into the archive
                                        Process cpProc = new ProcessBuilder( m_properties.getProperty("JACQImagesRPC.cpCommand"), m_properties.getProperty("JACQImagesRPC.cpCommandParameter"), inputFile.getPath(), archiveFile.getPath() ).start();
                                        int exitCode = cpProc.waitFor();
                                        cpProc.getErrorStream().close();
                                        cpProc.getInputStream().close();
                                        cpProc.getOutputStream().close();
                                        cpProc.destroy();
                                        if( exitCode == 0 ) {
                                            // Compare input and archive file
                                            if( inputFile.length() == archiveFile.length() ) {
                                                // Update archive resources list
                                                PreparedStatement archiveStat = m_conn.prepareStatement( "INSERT INTO `archive_resources` ( `identifier`, `imageFile` ) values (?, ?)" );
                                                archiveStat.setString(1, identifier);
                                                archiveStat.setString(2, archiveFile.getAbsolutePath() );
                                                archiveStat.executeUpdate();
                                                archiveStat.close();

                                                // Remove input file
                                                inputFile.delete();
                                            }
                                            else {
                                                throw new TransformException( "Validity check of file failed [" + inputFile.getPath() + " => " + archiveFile.getPath() + "]" );
                                            }
                                        }
                                        else {
                                            throw new TransformException( "Unable to move file into archive [" + inputFile.getPath() + " => " + archiveFile.getPath() + "]" );
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
                            throw new TransformException( "Identifier already exists [" + identifier + "]" );
                        }
                    }
                    catch( Exception e ) {
                        // Log this issue
                        PreparedStatement logStat = m_conn.prepareStatement( "INSERT INTO `import_logs` ( `it_id`, `logtime`, `identifier`, `message` ) values(?, ?, ?, ?)" );
                        logStat.setInt(1,it_id);
                        logStat.setString(2, String.valueOf(System.currentTimeMillis() / 1000) );
                        logStat.setString(3, identifier);
                        logStat.setString(4, e.getMessage() );
                        logStat.executeUpdate();
                        logStat.close();
                    }
                }
                // Release prepared statements
                existsStat.close();
            }
            catch( Exception e ) {
                System.err.println( e.toString() );
                //e.printStackTrace();
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
            HashMap<String,String> dirContent = listDirectory(m_properties.getProperty("JACQImagesRPC.resourcesDirectory"));

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
}
