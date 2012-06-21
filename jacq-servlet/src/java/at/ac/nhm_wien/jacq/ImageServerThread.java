/**
 * Basic thread implementation which supports listeners
 */
package at.ac.nhm_wien.jacq;

import java.util.ArrayList;

/**
 *
 * @author wkoller
 */
public abstract class ImageServerThread extends Thread {
    private ArrayList<ThreadListener> m_listeners = new ArrayList<ThreadListener>();
    private int thread_id = 0;
    
    public abstract int getThread_type();

    public int getThread_id() {
        return thread_id;
    }

    public void setThread_id(int thread_id) {
        this.thread_id = thread_id;
    }
    
    public void registerListener(ThreadListener listener) {
        this.m_listeners.add(listener);
    }
    
    public void removeListener(ThreadListener listener) {
        this.m_listeners.remove(listener);
    }
    
    protected void notifyListeners() {
        for( ThreadListener listener : m_listeners ) {
            listener.threadCompleted(this);
        }
    }
    
    protected void messageListeners(String message) {
        this.messageListeners("", message);
    }
    
    protected void messageListeners(String identifier, String message) {
        for( ThreadListener listener : m_listeners ) {
            listener.threadMessage(this, identifier, message);
        }
    }
}
