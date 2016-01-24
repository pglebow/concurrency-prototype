/**
 * 
 */
package com.glebow.proto;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author pglebow
 *
 */
public class ConcurrencyTest {

    private CountDownLatch latch;
    private LinkedList<String> hosts = Lists.newLinkedList();
    private int workersPerHost = 4;

    @Test
    public void test() throws InterruptedException {

        hosts.add("One");
        hosts.add("Two");
        hosts.add("Three");
        hosts.add("Four");

        latch = new CountDownLatch(hosts.size());

        LinkedList<ListeningExecutorService> executors = new LinkedList<>();
        hosts.forEach(h -> {
            ThreadFactoryBuilder b = new ThreadFactoryBuilder();
            b.setNameFormat(h + "-worker-%d");
            NamedThreadPoolExecutor e = new NamedThreadPoolExecutor(workersPerHost, workersPerHost, 0L,
                    TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), b.build());
            e.setName(h);
            e.setLatch(latch);
            executors.add(MoreExecutors.listeningDecorator(e));
        });

        for (int i = 0; i < hosts.size(); i++) {
            ListeningExecutorService e = executors.get(i);
            for (int j = 0; j < workersPerHost; j++) {
                Futures.addCallback(e.submit(new Task(String.valueOf(j))), new FutureCallback<Result>() {

                    @Override
                    public void onSuccess(Result result) {
                        System.out.println("Success: " + result.toString());

                    }

                    @Override
                    public void onFailure(Throwable t) {
                        System.out.println("Failure: " + t.getMessage());

                    }
                });
            }
            e.shutdown();
        }

        latch.await();
        System.out.println("Process complete!");
    }

}
