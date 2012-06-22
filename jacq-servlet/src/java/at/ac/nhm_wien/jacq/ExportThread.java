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
public class ExportThread extends ImageServerThread {
    private String m_exportPath = null;
    private JSONArray m_identifiers = null;

    /**
     * Create thread and pass list of identifier to export
     * @param identifiers 
     */
    public ExportThread( String exportPath, JSONArray identifiers ) throws Exception {
        super(2);
        
        m_exportPath = exportPath;
        m_identifiers = identifiers;
        
        // Check for trailing slash
        if( !m_exportPath.endsWith("/") ) m_exportPath += "/";
    }

    /**
     * Export the given list of images and copy them to the external location
     */
    @Override
    public void run() {
        try {
            // Prepare statement for fetching archive path
            PreparedStatement archiveSelect = m_conn.prepareStatement("SELECT `imageFile` FROM `archive_resources` WHERE `identifier` = ?");

            // Fetch paths for identifiers
            for( int i = 0; i < m_identifiers.size(); i++ ) {
                String identifier = m_identifiers.getString(i);

                archiveSelect.setString(1, identifier);
                ResultSet rs = archiveSelect.executeQuery();

                // Check if we have a result
                if( rs.next() ) {
                    File archiveFile = new File(ImageServer.m_properties.getProperty("ImageServer.archiveDirectory"), rs.getString("imageFile"));

                    // Insert entry into export queue
                    PreparedStatement queueInsert = m_conn.prepareStatement("INSERT INTO `export_queue` ( `archiveFilePath`, `exportFilePath` ) values (?, ?)");
                    queueInsert.setString(1, archiveFile.getAbsolutePath() );
                    queueInsert.setString(2, m_exportPath + archiveFile.getName() );
                    queueInsert.executeUpdate();
                    queueInsert.close();
                }
                else {
                    this.logMessage(identifier, "Unable to find file for identifier");
                }
                rs.close();
            }
            archiveSelect.close();

            while( true ) {
                // Select all files waiting for export
                PreparedStatement queueStmt = m_conn.prepareStatement("SELECT * FROM `export_queue` LIMIT 1");
                ResultSet rs = queueStmt.executeQuery();
                
                // Check if no more entries are left
                if( !rs.next() ) break;
                
                int eq_id = rs.getInt("eq_id");
                String archiveFilePath =  rs.getString("archiveFilePath");
                String exportFilePath = rs.getString("exportFilePath");
                
                // Close resultset to release the table
                rs.close();
                queueStmt.close();
                
                try {
                    // Copy file to destination
                    Utilities.copyFile(archiveFilePath, exportFilePath);
                }
                catch( Exception e ) {
                    this.logMessage("Unable to copy file [" + archiveFilePath + "] to export target: " + e.getMessage());
                }

                // Remove entry from export queue
                PreparedStatement queueDeleteStmt = m_conn.prepareStatement("DELETE FROM `export_queue` WHERE `eq_id` = ?");
                queueDeleteStmt.setInt(1, eq_id);
                queueDeleteStmt.executeUpdate();
                queueDeleteStmt.close();
            }
        }
        catch( Exception e ) {
            this.logMessage(e.getMessage());
            e.printStackTrace();
        }
        
        // Make sure the super function runs as well (which does the cleanup)
        super.run();
    }
}
