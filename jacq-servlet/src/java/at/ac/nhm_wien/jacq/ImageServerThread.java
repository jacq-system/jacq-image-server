/**
 * Basic thread implementation which supports listeners
 */
package at.ac.nhm_wien.jacq;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

/**
 *
 * @author wkoller
 */
public abstract class ImageServerThread extends Thread {
    private int m_thread_id = 0;
    private int m_thread_type = 0;
    
    protected Connection m_conn = null;

    public ImageServerThread() throws Exception {
        throw new Exception( "Default constructor not supported" );
    }

    /**
     * Initialize thread & create database entry
     * @param thread_type
     * @throws Exception 
     */
    public ImageServerThread(int thread_type) throws Exception {
        m_conn = Utilities.getConnection();
        m_thread_type = thread_type;
        
        // Insert start entry into database and fetch internal thread id
        PreparedStatement etInsert = m_conn.prepareStatement( "INSERT INTO `threads` ( `thread_id`, `starttime`, `type` ) values ( ?, ?, ? )" );
        etInsert.setLong(1, this.getId());
        etInsert.setLong(2, System.currentTimeMillis() / 1000);
        etInsert.setInt(3, m_thread_type);
        etInsert.executeUpdate();

        // Fetch auto-increment value and assign the unique number to our thread
        ResultSet et_id_result = etInsert.getGeneratedKeys();
        if( !et_id_result.next() ) {
            throw new Exception( "Unable to log export thread." );
        }
        
        // Remember internal thread id
        m_thread_id = et_id_result.getInt(1);

        // Close database handlers
        et_id_result.close();
        etInsert.close();
    }

    public int getThread_type() {
        return m_thread_type;
    }

    public int getThread_id() {
        return m_thread_id;
    }
    
    /**
     * Log a message to the database
     * @param identifier
     * @param message 
     */
    public void logMessage(String identifier, String message) {
        long logtime = System.currentTimeMillis() / 1000;
        
        try {
            // Log the message to the database
            PreparedStatement logInsert = m_conn.prepareStatement("INSERT INTO `thread_logs` ( `t_id`, `logtime`, `identifier`, `message` ) values (?, ?, ? ,?)");
            logInsert.setInt(1, this.m_thread_id);
            logInsert.setLong(2, logtime);
            logInsert.setString(3, identifier);
            logInsert.setString(4, message);
            logInsert.executeUpdate();
            logInsert.close();
        }
        catch( Exception e ) {
            // If something went wrong, at least log the message to output
            System.err.println( "Unable to log threadMessage to database!" );
            System.err.println( "[t_id: " + this.m_thread_id + "] [ogtime: " + logtime + "] [identifier: " + identifier + "] [message: " + message + "]" );
            e.printStackTrace();
        }
    }
    
    /**
     * Log message function for convience (if no identifier is present)
     * @param message 
     */
    public void logMessage(String message) {
        this.logMessage("", message);
    }

    /**
     * Cleans up the running thread, must be called from the sub-class just before run() finishes
     */
    @Override
    public void run() {
        long endtime = System.currentTimeMillis() / 1000;
        
        try {
            // Update thread table to finish this thread
            PreparedStatement threadUpdate = m_conn.prepareStatement("UPDATE `threads` SET `endtime` = ? WHERE `t_id` = ?");
            threadUpdate.setLong(1, endtime);
            threadUpdate.setInt(2, m_thread_id);
            threadUpdate.executeUpdate();
            threadUpdate.close();
        }
        catch( Exception e ) {
            System.err.println( "Unable to update thread table [" + m_thread_id + "]" );
            e.printStackTrace();
        }
        
        try {
            m_conn.close();
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
    }
}
