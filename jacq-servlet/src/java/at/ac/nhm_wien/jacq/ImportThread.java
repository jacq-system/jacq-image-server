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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author wkoller
 */
public class ImportThread extends ImageServerThread {

    public ImportThread() throws Exception {
        super(1);
    }

    @Override
    public void run() {
        try {
            int iqCount = 0;    // Import-Queue count

            // Disable auto-commit during imports
            m_conn.setAutoCommit(false);
            
            // Check if there are items waiting in the queue (e.g. due to a crash)...
            PreparedStatement queueStat = m_conn.prepareStatement("SELECT count(*) FROM `import_queue`");
            ResultSet rs = queueStat.executeQuery();
            if( rs.next() ) iqCount = rs.getInt(1);
            rs.close();
            queueStat.close();

            // If no items are waiting, fetch a list of fresh entries from the file-system
            if( iqCount <= 0 ) {
                try {
                    // Get a list of images to import
                    HashMap<String,String> importContent = Utilities.listDirectory(ImageServer.m_properties.getProperty("ImageServer.importDirectory"));

                    // Cache list in database table
                    queueStat = m_conn.prepareStatement("INSERT INTO `import_queue` (`identifier`, `filePath`) values (?, ?)");
                    Iterator<Map.Entry<String,String>> icIt = importContent.entrySet().iterator();
                    // Cycle through results and store them in the database
                    while( icIt.hasNext() ) {
                        Map.Entry<String,String> entry = icIt.next();
                        String identifier = entry.getKey();
                        String inputName = entry.getValue();

                        // Assign properties to statement
                        queueStat.setString(1, identifier);
                        queueStat.setString(2, inputName);
                        queueStat.addBatch();
                    }
                    // Finally execute & commit the queue-list
                    queueStat.executeBatch();
                    m_conn.commit();
                    queueStat.close();
                }
                catch( Exception e ) {
                    this.logMessage("Unable to fill import-queue: " + e.getMessage());
                }
            }
            
            // image server without archive has to use the resources table
            String existsStatSQL = "SELECT `identifier` FROM `archive_resources` WHERE `identifier` = ? AND `obsolete` = 0";
            if( Boolean.parseBoolean(ImageServer.m_properties.getProperty("ImageServer.noArchive")) ) {
                existsStatSQL = "SELECT `identifier` FROM `resources` WHERE `identifier` = ?";
            }

            // Prepare statement for identifier check
            PreparedStatement existsStat = m_conn.prepareStatement( existsStatSQL );
            // Find all entries in queue
            queueStat = m_conn.prepareStatement("SELECT * FROM `import_queue`");
            ResultSet statRs = queueStat.executeQuery();
            // Fetch entries into temporary memory
            HashMap<String, HashMap<String, Object>> importQueue = new HashMap<String, HashMap<String, Object>>();
            while( statRs.next() ) {
                HashMap<String, Object> queueObj = new HashMap<String, Object>();
                queueObj.put( "filePath", statRs.getString("filePath") );
                queueObj.put( "force", statRs.getInt("force") );

                importQueue.put(statRs.getString("identifier"), queueObj);
            }
            statRs.close();
            queueStat.close();

            // Iterate over entries list and process them
            Iterator<Map.Entry<String,HashMap<String, Object>>> iqIt = importQueue.entrySet().iterator();
            while( iqIt.hasNext() ) {
                // Fetch important information from queue-list
                Map.Entry<String,HashMap<String, Object>> entry = iqIt.next();
                String identifier = entry.getKey();
                HashMap<String, Object> queueObj = entry.getValue();
                String inputName = (String) queueObj.get("filePath");
                int force = (Integer) queueObj.get("force");

                // Start working on entry
                try {
                    // Check if the identifier already exists
                    existsStat.setString(1, identifier);
                    rs = existsStat.executeQuery();
                    boolean status = rs.next();
                    rs.close();
                    // check if file exists and we do not force
                    if( status && force != 1 ) {
                        throw new Exception( "Import of [" + inputName + "] failed. Identifier already exists [" + identifier + "]" );
                    }

                    // Create output file-name & path
                    String temporaryName = ImageServer.m_properties.getProperty("ImageServer.tempDirectory") + identifier + ".tif";

                    // Check if we want to watermark the image
                    if( !ImageServer.m_properties.getProperty("ImageServer.watermark").isEmpty() ) {
                        // Watermark the image
                        // Note: [0] for the input-file in order to avoid conflicts with multi-page tiffs
                        Process watermarkProc = new ProcessBuilder( ImageServer.m_properties.getProperty("ImageServer.imComposite"), "-quiet", "-gravity", "SouthEast", ImageServer.m_properties.getProperty("ImageServer.watermark"), inputName + "[0]", temporaryName ).start();
                        watermarkProc.waitFor();
                        watermarkProc.getErrorStream().close();
                        watermarkProc.getInputStream().close();
                        watermarkProc.getOutputStream().close();
                        watermarkProc.destroy();
                    }
                    else {
                        // No temporary file, so use input directly
                        temporaryName = inputName;
                    }

                    // Check if the watermarking was successfull
                    File temporaryFile = new File(temporaryName);
                    if( !temporaryFile.exists() ) {
                        throw new Exception( "Watermarking image failed [" + temporaryFile.getPath() + "]" );
                    }
                        
                    // Create output directory for djatoka
                    File inputFile = new File(inputName);
                    String outputName = Utilities.createDirectory(ImageServer.m_properties.getProperty("ImageServer.resourcesDirectory"), Utilities.getDirectoryName(inputFile.lastModified())) + identifier + ".jp2";

                    // Convert new image
                    Process compressProc = new ProcessBuilder( ImageServer.m_properties.getProperty("ImageServer.dCompress"), "-i", temporaryName, "-o", outputName ).start();
                    compressProc.waitFor();
                    compressProc.getErrorStream().close();
                    compressProc.getInputStream().close();
                    compressProc.getOutputStream().close();
                    compressProc.destroy();

                    // Remove temporary file
                    temporaryFile.delete();

                    // Check if image conversion was successfull
                    File outputFile = new File(outputName);
                    if( !outputFile.exists() ) {
                        throw new Exception( "Writing file for Djatoka failed [" + outputFile.getPath() + "]" );
                    }
                    // Check if an old entry already existed
                    if( status ) {
                        // Remove old resources entry
                        PreparedStatement resDelStat = m_conn.prepareStatement("DELETE FROM `resources` WHERE `identifier` = ?");
                        resDelStat.setString(1, identifier);
                        resDelStat.executeUpdate();
                        resDelStat.close();
                    }

                    // Insert id into database
                    PreparedStatement resourcesStat = m_conn.prepareStatement( "INSERT INTO `resources` ( `identifier`, `imageFile` ) values (?, ?)" );
                    resourcesStat.setString(1, identifier);
                    resourcesStat.setString(2, outputName );
                    resourcesStat.executeUpdate();
                    resourcesStat.close();

                    // check if archiving is desired
                    if( !Boolean.parseBoolean(ImageServer.m_properties.getProperty("ImageServer.noArchive")) ) {
                        // Create archive directory
                        String archiveDirectory = Utilities.getDirectoryName(inputFile.lastModified());
                        File archiveFile = new File( Utilities.createDirectory(ImageServer.m_properties.getProperty("ImageServer.archiveDirectory"), archiveDirectory ) + inputFile.getName() );

                        // Check if destination does not exist (or we are forcing the import)
                        if( archiveFile.exists() && force != 1 ) {
                            throw new Exception( "File already exists in archive [" + archiveFile.getPath() + "]" );
                        }
                            
                        // Check if the archive path can be written
                        if( !archiveFile.getParentFile().canWrite() ) {
                            throw new Exception( "Unable to write archive directory [" + archiveFile.getPath() + "]" );
                        }

                        // Copy the file into the archive
                        try {
                            Utilities.copyFile(inputFile.getPath(), archiveFile.getPath());

                            // Check if an old entry already existed
                            if( status ) {
                                // Mark old entry as obsolete
                                PreparedStatement archiveUpdateStat = m_conn.prepareStatement("UPDATE `archive_resources` SET `obsolete` = 1 WHERE `identifier` = ?");
                                archiveUpdateStat.setString(1, identifier);
                                archiveUpdateStat.executeUpdate();
                                archiveUpdateStat.close();
                            }

                            // Update archive resources list
                            PreparedStatement archiveStat = m_conn.prepareStatement( "INSERT INTO `archive_resources` ( `identifier`, `imageFile`, `lastModified`, `size`, `it_id` ) values (?, ?, ?, ?, ?)" );
                            archiveStat.setString(1, identifier);
                            archiveStat.setString(2, archiveDirectory + archiveFile.getName());
                            archiveStat.setLong(3, inputFile.lastModified() / 1000);    // NOTE: Using inputFile here to make sure we get the correct information
                            archiveStat.setLong(4, inputFile.length());
                            archiveStat.setInt(5, this.getThread_id());
                            archiveStat.executeUpdate();
                            archiveStat.close();

                            // Remove input file
                            inputFile.delete();
                        }
                        catch( Exception e ) {
                            throw new Exception( "Unable to move file into archive [" + inputFile.getPath() + " => " + archiveFile.getPath() + "] - Error: [" + e.getMessage() + "]" );
                        }
                    }

                    // Finally make sure our new data is written to disk
                    m_conn.commit();
                }
                catch( Exception e ) {
                    // print stack trace
                    e.printStackTrace();

                    // Something went wrong, rollback (this might fail, if no transaction started yet, so be tolerant)
                    try { m_conn.rollback(); } catch( Exception ex ) { System.err.println("Rollback failed: " + ex.getMessage() ); }

                    // Write import errors to application server log (for tracing errors)
                    this.logMessage(identifier, e.getMessage());

                    // Commit the log message
                    m_conn.commit();
                }
                // Remove item from import queue
                // This is done in any case since else it will stay in the queue and block it forever...
                queueStat = m_conn.prepareStatement("DELETE FROM `import_queue` WHERE `identifier` = ?");
                queueStat.setString(1, identifier);
                queueStat.executeUpdate();
                queueStat.close();
                m_conn.commit();
            }
            // Release prepared statements
            existsStat.close();
            // Enable auto-commit
            m_conn.setAutoCommit(true);
        }
        catch(SQLException e ) {
            System.err.println( "[" + e.getErrorCode() + "] " + e.getSQLState() + " [" + e.getMessage() + "]");
            e.printStackTrace();
        }
        catch( Exception e ) {
            System.err.println( e.toString() );
            e.printStackTrace();
        }

        // Make sure the super function runs as well (which does the cleanup)
        super.run();
    }
}
