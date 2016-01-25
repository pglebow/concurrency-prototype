/**
 * 
 */
package com.glebow.proto;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;

import lombok.extern.slf4j.Slf4j;

/**
 * @author pglebow
 *
 */
@Slf4j
public class ConcurrencyTest {

    private int workersPerHost = 4;

    private int numTasks = 17;

    private Set<String> hosts = Sets.newHashSet("One", "Two", "Three", "Four");
    
    private Executor callBackExecutor = Executors.newCachedThreadPool();

    @Test
    public void test() {
        try {
            Set<Task> tasks = new HashSet<Task>();
            for (int i = 0; i < numTasks; i++) {
                tasks.add(new Task(String.valueOf(i)));
            }

            OptimizationProcessor<String, Result, Task> p = new OptimizationProcessor<>(hosts, workersPerHost, tasks,
                    new Callback(), callBackExecutor);
            Duration d = p.process().get();
            log.info("Duration: " + d);            
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
    
    public class Callback implements FutureCallback<Result> {

        @Override
        public void onSuccess(Result result) {
            log.info(result.toString());
            
        }

        @Override
        public void onFailure(Throwable t) {
            log.error(t.getMessage(), t);
        }
        
    }

}
