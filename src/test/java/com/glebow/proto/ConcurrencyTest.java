/**
 * 
 */
package com.glebow.proto;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
    private int numberOfTasks = 2001;

    private List<String> toProcess;

    @Test
    public void test() throws InterruptedException {

        toProcess = new ArrayList<>(numberOfTasks);
        for (int i = 0; i < numberOfTasks; i++) {
            toProcess.add(String.valueOf(i));
        }

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

        List<List<String>> partitionedTasks = Lists.partition(toProcess, (toProcess.size() / workersPerHost));
        for (int i = 0; i < hosts.size(); i++) {
            ListeningExecutorService e = executors.get(i);
            for (int j = 0; j < workersPerHost; j++) {
                List<String> tasks = partitionedTasks.get(j);

                tasks.forEach(t -> {
                    System.out.println("Creating task " + t);
                    Futures.addCallback(e.submit(new Task(t)), new FutureCallback<Result>() {

                        @Override
                        public void onSuccess(Result result) {
                            System.out.println("Success: " + result.toString());
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            System.out.println("Failure: " + t.getMessage());
                        }
                    });
                });
            }
            e.shutdown();
        }

        latch.await();
        System.out.println("Process complete!");
    }

}
