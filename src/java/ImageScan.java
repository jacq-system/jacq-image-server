
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import net.sf.json.JSONObject;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wkoller
 */
public class ImageScan extends HttpServlet {
    private String m_sourceDirectory = null;
    private String m_dataDirectory = null;
    private String m_imageIndex = null;
    private String m_djatokaDirectory = null;
    private String m_archiveDirectory = null;

    private HashMap<String,String> m_sourceImages = new HashMap<String,String>();
    private HashMap<String,String> m_dataImages = new HashMap<String,String>();
    private HashMap<String,String> m_archiveImages = new HashMap<String,String>();
    
    private Properties m_properties = new Properties();
    
    @Override
    public void init() throws ServletException {
        try {
            m_properties.load(new FileInputStream(getServletContext().getRealPath( "/WEB-INF/ImageScan.properties" )));

            m_sourceDirectory = m_properties.getProperty( "ImageScan.sourceDirectory" );
            m_dataDirectory = m_properties.getProperty( "ImageScan.dataDirectory" );
            m_imageIndex = m_properties.getProperty( "ImageScan.imageIndex" );
            m_djatokaDirectory = m_properties.getProperty( "ImageScan.djatokaDirectory" );
            m_archiveDirectory = m_properties.getProperty( "ImageScan.archiveDirectory" );
            
            rescanImages();
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        PrintWriter out = resp.getWriter();
        
        // Read the request input
        BufferedReader in = req.getReader();
        String inputString = "";
        for( int readChar = in.read(); readChar >= 0; readChar = in.read() ) {
            inputString += (char) readChar;
        }
        
        // Define our function parameters
        String method = null;
        String params = null;
        String id = null;
        
        // Convert the input to a JSONObject
        JSONObject input = null;
        try {
            input = JSONObject.fromObject(inputString);
        }
        catch( Exception e ) {
        }
        if( input != null && !input.isNullObject() ) {
            method = req.getParameter( "method" );
            params = req.getParameter( "params" );
            id = req.getParameter( "id" );
        }
        // Be graceful and try to fetch the parameters from GET
        else {
            method = req.getParameter( "method" );
            params = req.getParameter( "params" );
            id = req.getParameter( "id" );
        }
        
        // Handle the request and receive the response
        JSONObject response = handleRequest(method, JSONObject.fromObject(params));
        response.element( "id", id );
        
        // Send response back to caller
        out.print( response );
    }
    
    private JSONObject handleRequest( String method, JSONObject params ) {
        JSONObject response = new JSONObject();
        
        if( method == null ) {
            response.element( "status", false );
        }
        else if( method.equals("list") ) {
            response.element( "status", true );
            if( !params.isNullObject() && params.getBoolean("archive") ) {
                response.element( "files", m_archiveImages );
            }
            else {
                response.element( "files", m_dataImages );
            }
        }
        else if( method.equals( "scan" ) ) {
            boolean status = rescanImages();
            response.element( "status", status );
        }
        else if( method.equals( "import" ) ) {
            boolean status = importImages();
            response.element( "status", status );
        }
        else if( method.equals( "sync" ) ) {
        }
        
        return response;
    }
    
    private boolean rescanImages() {
        m_dataImages.clear();
        m_archiveImages.clear();
        
        m_dataImages = recursiveScanDir( "", new File( m_dataDirectory ));
        m_archiveImages = recursiveScanDir( "", new File( m_archiveDirectory ));
        
        return true;
    }
    
    private JSONObject resyncImages() {
        // First of all update images lists
        boolean status = this.rescanImages();
        
        // Check if re-scanning worked
        if( !status ) return null;
        
        Iterator<Map.Entry<String,String>> aiIt = m_archiveImages.entrySet().iterator();
        while( aiIt.hasNext() ) {
            Map.Entry<String,String> currEntry = aiIt.next();
            
            // Check if the image exists in djatoka
            if( !m_dataImages.containsKey( currEntry.getKey().concat(".jp2") ) ) {
                
            }
        }

        return null;
    }
    
    private boolean importImages() {
        // Refresh incoming files
        m_sourceImages = recursiveScanDir( "", new File( m_sourceDirectory ));
        
        // Convert any missing files
        try {
            Iterator<Map.Entry<String,String>> siIt = m_sourceImages.entrySet().iterator();
            while(siIt.hasNext()) {
                Map.Entry<String,String> currEntry = siIt.next();
                
                // Check if image already exists in archive
                if( m_archiveImages.get( currEntry.getKey() ) != null ) {
                    System.err.println( "ERROR: Image '" + currEntry.getKey() + "' already exists!" );
                    continue;
                }
                //if( path != null && currEntry.getValue().equals(path) ) continue;
                
                // Create the dir(s), if necessary
                File dataDir = new File( m_dataDirectory.concat(currEntry.getValue()) );
                if( !dataDir.exists() && !dataDir.mkdirs() ) {
                    System.err.println( "ERROR: Can't create data dir: " + dataDir.getPath() );
                    return false;
                }
                File archiveDir = new File( m_archiveDirectory.concat(currEntry.getValue()) );
                if( !archiveDir.exists() && !archiveDir.mkdirs() ) {
                    System.err.println( "ERROR: Can't create archive dir: " + archiveDir.getPath() );
                    return false;
                }
                
                // Create the source path
                String sourcePath = m_sourceDirectory.concat( currEntry.getValue() ).concat( currEntry.getKey() );
                String dataPath = m_dataDirectory.concat(currEntry.getValue()).concat(currEntry.getKey()).concat(".jp2");
                
                // Finally run compress
                String[] compress = new String[]{ m_djatokaDirectory.concat( "bin/compress.sh" ), "-i", sourcePath, "-o", dataPath };
                Process compressProc = Runtime.getRuntime().exec(compress);
                compressProc.waitFor();
                
                // Now copy converted file to archive
                File sourceFile = new File(sourcePath);
                File archiveFile = new File(m_archiveDirectory.concat( currEntry.getValue() ).concat( currEntry.getKey() ));
                if( !sourceFile.renameTo(archiveFile) ) {
                    return false;
                }
                
                // Update the internal image lists
                m_archiveImages.put(currEntry.getKey(), currEntry.getValue());
                m_dataImages.put(currEntry.getKey().concat(".jp2"), currEntry.getValue());
            }
        
            // Update the image index file
            FileWriter writer = new FileWriter( m_imageIndex );
            
            siIt = m_dataImages.entrySet().iterator();
            while( siIt.hasNext() ) {
                Map.Entry<String,String> currEntry = siIt.next();
                writer.write( "info:" + currEntry.getKey() + "\t" + m_dataDirectory + currEntry.getValue() + currEntry.getKey() + "\n" );
            }
            writer.close();
        }
        catch( Exception e ) {
            return false;
        }
        
        return true;
    }
    
    private HashMap<String,String> recursiveScanDir( String path, File dir ) {
        HashMap<String,String> entries = new HashMap<String,String>();
        
        // Fetch all children
        File children[] = dir.listFiles();
        if( children == null ) return entries;
        
        // Cycle through children and list them
        for( int i = 0; i < children.length; i++ ) {
            if( children[i].isDirectory() ) {
                entries.putAll( recursiveScanDir( path + children[i].getName() + "/", children[i] ) );
            }
            else {
                entries.put( children[i].getName(), path );
            }
        }
        
        return entries;
    }
}
