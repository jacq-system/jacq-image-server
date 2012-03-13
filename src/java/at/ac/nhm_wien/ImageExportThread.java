/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package at.ac.nhm_wien;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import net.sf.json.JSONArray;

/**
 *
 * @author wkoller
 */
public class ImageExportThread extends Thread {
    /**
     * Create thread and pass list of identifier to export
     * @param identifiers 
     */
    public ImageExportThread( String archivePath, Connection conn, JSONArray identifiers ) {
        try {
            PreparedStatement archiveStmt = conn.prepareStatement("SELECT `imageFile` FROM `archive_resources` WHERE `identifier` = ?");

            for( int i = 0; i < identifiers.size(); i++ ) {
                String identifier = identifiers.getString(i);
                
                archiveStmt.setString(1, identifier);
                ResultSet rs = archiveStmt.executeQuery();
                
                // Check if we have a result
                if( rs.next() ) {
                    String archiveFileName = rs.getString("imageFile");
                }
            }
        }
        catch(Exception e) {
            System.err.println("[ImageExportThread] " + e.getMessage());
        }
    }

    /**
     * 
     */
    @Override
    public void run() {
    }
}
