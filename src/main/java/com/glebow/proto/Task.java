/**
 * 
 */
package com.glebow.proto;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

import lombok.Data;

/**
 * Simple task
 * 
 * @author pglebow
 *
 */
@Data
public class Task implements Callable<Result> {

    private String name;

    private LocalDateTime start;

    public Task(final String name) {
        start = LocalDateTime.now();
        this.name = name;
    }

    @Override
    public Result call() throws Exception {
        Thread.sleep((long) ((Math.random() + 1) * 5000));
        return new Result(name + " complete in " + Duration.between(start, LocalDateTime.now()));
    }

}
