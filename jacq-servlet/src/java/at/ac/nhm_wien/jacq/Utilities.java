/*
JACQ
Copyright (C) 2011-2013 Naturhistorisches Museum Wien

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
package at.ac.nhm_wien.jacq;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * @author wkoller
 */
public class Utilities {
    private static Connection m_connection = null;
    
    /**
     * Creates a new connection to the sqlite database
     * @return connection handler to sqlite database
     */
    public static Connection getConnection() {
        try {
            if( Utilities.m_connection == null ) {
                Class.forName("org.sqlite.JDBC");
                Utilities.m_connection = DriverManager.getConnection("jdbc:sqlite:" + ImageServer.m_properties.getProperty("ImageServer.database") );
                Utilities.m_connection.setAutoCommit(true);
            }
            
            return Utilities.m_connection;
        }
        catch( Exception e ) {
            System.err.println("Unable to open connection");
            e.printStackTrace();
            return null;
        }
    }
    
    /**
     * Creates the correctly formatted directory name for a given modification date
     * @param p_modificationDate modification date to creaste the directory name for
     * @return formatted directory name
     */
    public static String getDirectoryName(long p_modificationDate) {
        // Create formatted output directory
        SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy/yyMMdd/");
        Date fileDate = new Date(p_modificationDate);
        return yearFormat.format(fileDate);
    }

    /**
     * Small helper function which creates an output directory according to our
     * formatting rules
     *
     * @param p_baseDir Base dir for directory creation
     * @param archiveDir sub-directory within the archive
     * naming
     * @return String Returns the name of the output directory on success (with
     * trailing slash)
     * @throws TransformException
     */
    public static String createDirectory(String p_baseDir, String archiveDir) throws Exception {
        File baseDir = new File(p_baseDir);
        if (baseDir.exists() && baseDir.isDirectory() && baseDir.canWrite()) {
            File subDir = new File(baseDir, archiveDir);

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

    /**
     * Internal helper function for listing the content of a directory (+ creating IDs for the entries)
     * @param sourceDir Directory to list
     * @return HashMap of identifier => path entries
     */
    public static HashMap<String,String> listDirectory( String sourceDir ) {
        HashMap<String,String> dirContent = new HashMap<String,String>();
        
        File dir = new File( sourceDir );
        File dirEntries[] = dir.listFiles();
        
        for(int i = 0; i < dirEntries.length; i++ ) {
            File dirEntry = dirEntries[i];
            if( dirEntry.isDirectory() ) {
                dirContent.putAll( listDirectory(dirEntry.getAbsolutePath()) );
            }
            else if( dirEntry.isFile() ) {
                int extSepPosition = dirEntry.getName().lastIndexOf(".");
                if( extSepPosition < 0 ) {
                    System.err.println("Invalid file entry: " + dirEntry.getName());
                    continue;
                }
                
                dirContent.put(dirEntry.getName().substring(0, extSepPosition), dirEntry.getAbsolutePath());
            }
        }
        
        return dirContent;
    }    
}
