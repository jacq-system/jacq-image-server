/**
 * Interface which can be implemented if you want to be notified when a thread finishes
 */
package at.ac.nhm_wien.jacq;

/**
 *
 * @author wkoller
 */
public interface ThreadListener {
    public void threadCompleted( ImageServerThread thread );
    public void threadMessage( ImageServerThread thread, String identifier, String message );
}
