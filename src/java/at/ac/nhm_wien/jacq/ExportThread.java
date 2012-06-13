/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package at.ac.nhm_wien.jacq;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import net.sf.json.JSONArray;

/**
 *
 * @author wkoller
 */
public class ExportThread extends Thread {
    private String m_exportPath = null;
    private Connection m_conn = null;
    
    /**
     * Create thread and pass list of identifier to export
     * @param identifiers 
     */
    public ExportThread( String exportPath, JSONArray identifiers, Connection p_conn ) throws Exception {
        m_exportPath = exportPath;
        m_conn = p_conn;
        
        // Check for trailing slash
        if( !m_exportPath.endsWith("/") ) m_exportPath += "/";

        // Prepare statement for fetching archive path
        m_conn.setAutoCommit(true);
        PreparedStatement archiveStmt = m_conn.prepareStatement("SELECT `imageFile` FROM `archive_resources` WHERE `identifier` = ?");
        PreparedStatement queueStmt = m_conn.prepareStatement("INSERT INTO `export_queue` ( `archiveFilePath`, `exportFilePath` ) values (?, ?)");

        // Fetch paths for identifiers
        for( int i = 0; i < identifiers.size(); i++ ) {
            String identifier = identifiers.getString(i);

            archiveStmt.setString(1, identifier);
            ResultSet rs = archiveStmt.executeQuery();

            // Check if we have a result
            if( rs.next() ) {
                File imageFile = new File(rs.getString("imageFile"));
                
                queueStmt.setString(0, imageFile.getAbsolutePath());
                queueStmt.setString(1, m_exportPath + imageFile.getName() );
                queueStmt.executeUpdate();
            }
            rs.close();
        }
    }

    /**
     * Export the given list of images and copy them to the external location
     */
    @Override
    public void run() {
        // Iterate over archive files and export them
        /*Iterator<String> afnIt = m_archiveFileNames.iterator();
        while( afnIt.hasNext() ) {
            String archiveFileName = afnIt.next();
            File archiveFile = new File(archiveFileName);

            // Copy file to export path
            int exitCode = Utilities.copyFile(archiveFileName, m_exportPath + archiveFile.getName());
        }*/
    }
}
