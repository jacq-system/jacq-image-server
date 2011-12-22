package at.ac.nhm_wien;

/*
FReuD
Copyright (C) 2011 Naturhistorisches Museum Wien

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import java.util.Map;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author wkoller
 */
public class FReuDImagesRPC extends HttpServlet {
    private String m_requestId = "";
    private JSONArray m_requestParams = null;
    private JSONObject m_response = null;
    private Properties m_properties = new Properties();
    private Connection m_conn = null;
    
    private Thread m_importThread = null;
    
    @Override
    public void init() throws ServletException {
        try {
            // Load properties
            m_properties.load(new FileInputStream(getServletContext().getRealPath( "/WEB-INF/"+ getClass().getSimpleName() + ".properties" )));
            // Establish "connection" to SQLite database
            Class.forName("org.sqlite.JDBC");
            m_conn = DriverManager.getConnection("jdbc:sqlite:" + m_properties.getProperty("FReuDImagesRPC.database") );

            // Check if database is already initialized
            Statement stat = m_conn.createStatement();
            // Check for resources table
            ResultSet rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `resources` ( `r_id` CONSTRAINT `r_id_pk`, PRIMARY KEY, `identifier` CONSTRAINT `identifier_unique` UNIQUE ON CONFLICT FAIL, `imageFile` )" );
            }
            rs.close();
            // Check for import-log table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_logs' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `import_logs` ( `il_id` CONSTRAINT `il_id_pk` PRIMARY KEY, `thread_id`, `logtime` DEFAULT 0, `identifier`, `message` )" );
            }
            rs.close();
            // Check for thread table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'import_threads' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `import_threads` ( `it_id` CONSTRAINT `it_id_pk` PRIMARY KEY, `thread_id` CONSTRAINT `thread_id_unique` UNIQUE ON CONFLICT FAIL, `starttime` DEFAULT 0, `endtime` DEFAULT 0 )" );
            }
            rs.close();
            // Check for archive resources table
            rs = stat.executeQuery("SELECT name FROM sqlite_master WHERE name = 'archive_resources' AND type = 'table'");
            if( !rs.next() ) {
                stat.executeUpdate( "CREATE table `archive_resources` ( `ar_id` CONSTRAINT `ar_id_pk` PRIMARY KEY AUTOINCREMENT, `identifier`, `imageFile` )" );
            }
            rs.close();
        }
        catch( Exception e ) {
            System.err.println( e.getMessage() );
        }
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
                    Class[] paramsClass = new Class[m_requestParams.size()];
                    String[] params = (String[]) m_requestParams.toArray(new String[1]);
                    for( int i = 0; i < params.length; i++ ) {
                        paramsClass[i] = params[i].getClass();
                    }

                    this.getClass().getMethod(methodName, paramsClass ).invoke(this, m_requestParams.toArray());
                }
                else {
                    this.getClass().getMethod(methodName ).invoke(this, m_requestParams.toArray());
                }
            }
            catch(Exception e ) {
                m_response.element("result", "");
                m_response.element("error", "Unable to call requested method: " + e.getMessage() );
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
        if( m_importThread == null ) {
            m_importThread = new ImageImportThread();
            m_importThread.start();

            m_response.element( "result", "1" );
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
            
            m_response.put("result", resources);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * List all import threads
     * @param cutoff_date threads older than cutoff_date wont be returned
     */
    public void listImportThreads( int cutoff_date ) {
        try {
            JSONObject threads = new JSONObject();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `thread_id`, `starttime` FROM `import_threads` WHERE `starttime` >= ? ORDER BY `thread_id`");
            stat.setString(1, String.valueOf(cutoff_date) );
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                threads.put( rs.getString("thread_id"), rs.getString("starttime") );
            }
            rs.close();
            
            m_response.put("result", threads);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * Returns a list of log messages for a given thread-id
     * @param thread_id 
     */
    public void listImportLogs( int thread_id ) {
        try {
            JSONArray logs = new JSONArray();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `message` FROM `import_logs` WHERE `thread_id` >= ? ORDER BY `logtime` ASC");
            stat.setString(1, String.valueOf(thread_id) );
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                logs.add( rs.getString("message") );
            }
            rs.close();
            
            m_response.put("result", logs);
        }
        catch( Exception e ) {
            m_response.put("error", e.getMessage());
            m_response.put("result", "");
        }
    }
    
    /**
     * Returns a list of file identifiers for a given specimen
     * @param specimen_id Specimen ID
     * @param herbar_number Herbarnumber of specimen
     */
    public void listSpecimenImages( int specimen_id, int herbar_number ) {
        try {
            JSONArray images = new JSONArray();
            
            PreparedStatement stat = m_conn.prepareStatement("SELECT `identifier` FROM `resources` WHERE `identifier` LIKE ? OR `identifier` LIKE ? ORDER BY `identifier` ASC");
            stat.setString(1, String.valueOf(specimen_id) );
            stat.setString(2, String.valueOf(herbar_number) );
            
            ResultSet rs = stat.executeQuery();
            while(rs.next()) {
                images.add( rs.getString("identifier") );
            }
            rs.close();
            
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
        m_importThread = null;
    }

    /**
     * Import thread which imports newly added images
     */
    private class ImageImportThread extends Thread {
        @Override
        public void run() {
            try {
                // Get a list of images to import
                HashMap<String,String> importContent = listDirectory(m_properties.getProperty("FReuDImagesRPC.importDirectory"));
                
                Iterator<Map.Entry<String,String>> icIt = importContent.entrySet().iterator();
                PreparedStatement prepStat = m_conn.prepareStatement( "SELECT `identifier` FROM `resources` WHERE `identifier` = ?" );
                PreparedStatement insertStat = m_conn.prepareStatement( "INSERT INTO `resources` values (?, ?)" );
                PreparedStatement logStat = m_conn.prepareStatement( "INSERT INTO `logs_import` ( `logtime`, `identifier`, `message` ) values(?, ?, ?)" );
                while( icIt.hasNext() ) {
                    Map.Entry<String,String> entry = icIt.next();
                    prepStat.setString(1, entry.getKey());
                    
                    // Check if the identifier already exists
                    ResultSet rs = prepStat.executeQuery();
                    boolean status = rs.next();
                    rs.close();
                    if( !status ) {
                        // Create input and output file-names & paths
                        String inputName = entry.getValue();
                        String outputName = m_properties.getProperty("FReuDImagesRPC.resourcesDirectory") + entry.getKey() + ".jp2";
                        
                        // Convert new image
                        String[] compress = new String[]{ m_properties.getProperty("FReuDImagesRPC.djatokaDirectory").concat( "bin/compress.sh" ), "-i", inputName, "-o", outputName };
                        Process compressProc = Runtime.getRuntime().exec(compress);
                        compressProc.waitFor();
                        
                        // Insert id into database
                        insertStat.setString(1, entry.getKey());
                        insertStat.setString(2, outputName );
                        insertStat.executeUpdate();
                    }
                    else {
                        // Log this issue
                        logStat.setString(1, String.valueOf(System.currentTimeMillis() / 1000L) );
                        logStat.setString(2, entry.getKey());
                        logStat.setString(3, "Identifier already exists!" );
                        logStat.executeUpdate();
                    }
                }
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
            HashMap<String,String> dirContent = listDirectory(m_properties.getProperty("FReuDImagesRPC.resourcesDirectory"));

            // Cleanup old entries
            Statement stat = m_conn.createStatement();
            stat.execute("DELETE FROM `resources`");
            
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
