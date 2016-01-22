/**
 * 
 */
package com.glebow.proto;

import java.util.concurrent.Callable;

import lombok.Data;

/**
 * Simple task
 * @author pglebow
 *
 */
@Data
public class Task implements Callable<Result>{

    @Override
    public Result call() throws Exception {
        return new Result("Test complete");
    }

}
