/**
 * 
 */
package com.glebow.proto;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Sets;

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

    @Test
    public void test() {
        try {
            Set<Task> tasks = new HashSet<Task>();
            for (int i = 0; i < numTasks; i++) {
                tasks.add(new Task(String.valueOf(i)));
            }

            OptimizationProcessor<String, Task> p = new OptimizationProcessor<String, Task>(hosts, workersPerHost, tasks);
            Duration d = p.process().get();
            log.info("Duration: " + d);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
