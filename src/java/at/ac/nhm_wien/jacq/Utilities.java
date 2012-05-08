/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package at.ac.nhm_wien.jacq;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.xml.crypto.dsig.TransformException;

/**
 *
 * @author wkoller
 */
public class Utilities {
        /**
         * Small helper function which creates an output directory according to our formatting rules
         * @param p_baseDir Base dir for directory creation
         * @param p_modificationDate Modification time which is used for directory naming
         * @return String Returns the name of the output directory on success (with trailing slash)
         * @throws TransformException 
         */
        public static String createDirectory( String p_baseDir, long p_modificationDate ) throws TransformException {
            File baseDir = new File(p_baseDir);
            if( baseDir.exists() && baseDir.isDirectory() && baseDir.canWrite() ) {
                // Create formatted output directory
                SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy/yyMMdd/");
                Date now =  new Date(p_modificationDate);
                File subDir = new File(baseDir, yearFormat.format(now));
                
                // Check if subDir already exists or try to create it
                if( subDir.exists() || subDir.mkdirs() ) {
                    return subDir.getPath() + "/";
                }
                else {
                    throw new TransformException( "Unable to access sub-dir [" + subDir.getPath() + "]" );
                }
            }
            else {
                throw new TransformException( "Unable to access base-dir [" + baseDir.getPath() + "]" );
            }
        }
        
        /**
         * Safely copy a file from the source to the destination using native tools
         * @param p_sourceFileName Name of source file
         * @param p_destFileName Name of destination file
         * @return Status code of copy command, or -1 on exception
         */
        public static int copyFile( String p_sourceFileName, String p_destFileName ) {
            int exitCode = -1;
            try {
                Process cpProc = new ProcessBuilder( ImageServer.m_properties.getProperty("JACQImagesRPC.cpCommand"), ImageServer.m_properties.getProperty("JACQImagesRPC.cpCommandParameter"), p_sourceFileName, p_destFileName ).start();
                exitCode = cpProc.waitFor();
                cpProc.getErrorStream().close();
                cpProc.getInputStream().close();
                cpProc.getOutputStream().close();
                cpProc.destroy();
            }
            catch(Exception e) {
                System.err.println("[Utilities.copyFile]" + e.getMessage());
                exitCode = -1;
            }

            return exitCode;
        }
    
}
