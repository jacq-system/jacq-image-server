/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package at.ac.nhm_wien.jacq;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
                
                queueStmt.setString(1, imageFile.getAbsolutePath());
                queueStmt.setString(2, m_exportPath + imageFile.getName() );
                queueStmt.executeUpdate();
            }
            else {
                System.err.println( "Unable to find file for identifier [" + identifier + "]" );
            }
            rs.close();
        }
    }

    /**
     * Export the given list of images and copy them to the external location
     */
    @Override
    public void run() {
        // Select all files waiting for export
        try {
            PreparedStatement queueStmt = m_conn.prepareStatement("SELECT * FROM `export_queue` LIMIT 1");
            PreparedStatement queueDeleteStmt = m_conn.prepareStatement("DELETE FROM `export_queue` WHERE `eq_id` = ?");
            while( true ) {
                ResultSet rs = queueStmt.executeQuery();
                
                // Check if no more entries are left
                if( !rs.next() ) break;
                
                int eq_id = rs.getInt("eq_id");
                String archiveFilePath = rs.getString("archiveFilePath");
                String exportFilePath = rs.getString("exportFilePath");
                
                // Close resultset to release the table
                rs.close();
                
                try {
                    // Copy file to destination
                    Utilities.copyFile(archiveFilePath, exportFilePath);

                    // Remove entry from export queue
                    queueDeleteStmt.setInt(1, eq_id);
                    queueDeleteStmt.executeUpdate();
                    m_conn.commit();
                }
                catch( Exception e ) {
                    System.err.println( "Unable to copy file [" + eq_id + "] to export target: " + e.getMessage() );
                }
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
    }
}
