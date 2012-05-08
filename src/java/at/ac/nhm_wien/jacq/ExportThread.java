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
    private ArrayList<String> m_archiveFileNames = new ArrayList<String>();
    private String m_exportPath = null;
    private Connection m_conn = null;
    
    /**
     * Create thread and pass list of identifier to export
     * @param identifiers 
     */
    public ExportThread( String exportPath, JSONArray identifiers ) throws Exception {
        m_exportPath = exportPath;
        
        // Check for trailing slash
        if( m_exportPath.charAt(m_exportPath.length() - 1) != '/' ) m_exportPath += "/";

        // Establish "connection" to SQLite database
        Class.forName("org.sqlite.JDBC");
        m_conn = DriverManager.getConnection("jdbc:sqlite:" + ImageServer.m_properties.getProperty("JACQImagesRPC.database") );
        m_conn.setAutoCommit(true);

        PreparedStatement archiveStmt = m_conn.prepareStatement("SELECT `imageFile` FROM `archive_resources` WHERE `identifier` = ?");

        for( int i = 0; i < identifiers.size(); i++ ) {
            String identifier = identifiers.getString(i);

            archiveStmt.setString(1, identifier);
            ResultSet rs = archiveStmt.executeQuery();

            // Check if we have a result
            if( rs.next() ) {
                m_archiveFileNames.add(rs.getString("imageFile"));
            }
        }
    }

    /**
     * Export the given list of images and copy them to the external location
     */
    @Override
    public void run() {
        // Iterate over archive files and export them
        Iterator<String> afnIt = m_archiveFileNames.iterator();
        while( afnIt.hasNext() ) {
            String archiveFileName = afnIt.next();
            File archiveFile = new File(archiveFileName);

            // Copy file to export path
            int exitCode = Utilities.copyFile(archiveFileName, m_exportPath + archiveFile.getName());
        }
    }
}
