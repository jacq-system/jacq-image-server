/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package at.ac.nhm_wien.jacq;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author wkoller
 */
public class Utilities {
    /**
     * Creates a new connection to the sqlite database
     * @return connection handler to sqlite database
     */
    public static Connection getConnection() {
        try {
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + ImageServer.m_properties.getProperty("ImageServer.database") );
            conn.setAutoCommit(true);
            return conn;
        }
        catch( Exception e ) {
            System.err.println("Unable to open connection");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Small helper function which creates an output directory according to our
     * formatting rules
     *
     * @param p_baseDir Base dir for directory creation
     * @param p_modificationDate Modification time which is used for directory
     * naming
     * @return String Returns the name of the output directory on success (with
     * trailing slash)
     * @throws TransformException
     */
    public static String createDirectory(String p_baseDir, long p_modificationDate) throws Exception {
        File baseDir = new File(p_baseDir);
        if (baseDir.exists() && baseDir.isDirectory() && baseDir.canWrite()) {
            // Create formatted output directory
            SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy/yyMMdd/");
            Date now = new Date(p_modificationDate);
            File subDir = new File(baseDir, yearFormat.format(now));

            // Check if subDir already exists or try to create it
            if (subDir.exists() || subDir.mkdirs()) {
                return subDir.getPath() + "/";
            } else {
                throw new Exception("Unable to access sub-dir [" + subDir.getPath() + "]");
            }
        } else {
            throw new Exception("Unable to access base-dir [" + baseDir.getPath() + "]");
        }
    }

    /**
     * Safely copy a file from the source to the destination using native tools
     *
     * @param p_sourceFileName Name of source file
     * @param p_destFileName Name of destination file
     * @return Status code of copy command, or -1 on exception
     */
    public static void copyFile(String p_sourceFileName, String p_destFileName) throws Exception {
        // Create copy command based on settings of image-server
        Process cpProc = new ProcessBuilder(ImageServer.m_properties.getProperty("ImageServer.cpCommand"), ImageServer.m_properties.getProperty("ImageServer.cpCommandParameter"), p_sourceFileName, p_destFileName).start();
        int exitCode = cpProc.waitFor();
        cpProc.getErrorStream().close();
        cpProc.getInputStream().close();
        cpProc.getOutputStream().close();
        cpProc.destroy();

        // Check if file copied correctly
        File sourceFile = new File(p_sourceFileName);
        File destFile = new File(p_destFileName);
        // Check file size
        if (sourceFile.length() != destFile.length()) {
            throw new Exception("File sizes did not match");
        }

        if (exitCode != 0) {
            throw new Exception("Copy command did not finish correctly. Returned exit code is '" + exitCode + "'");
        }
    }
}
