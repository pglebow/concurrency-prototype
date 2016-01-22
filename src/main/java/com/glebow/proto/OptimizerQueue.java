/**
 * 
 */
package com.glebow.proto;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author pglebow
 *
 */
public class OptimizerQueue {

    private BlockingQueue<Task> queue;
    
    /**
     * Default
     */
    public OptimizerQueue() {
        queue = new LinkedBlockingQueue<>();
    }
}
