/**
 * 
 */
package com.glebow.proto;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.istack.internal.NotNull;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author pglebow
 *
 */
@Slf4j
public class OptimizationProcessor<H extends Comparable<H>, R extends Callable<Result>> {

    @NotNull
    protected Collection<H> hosts;

    @NotNull
    protected Collection<R> tasks;

    @NotNull
    protected int workersPerHost;

    /** Executor for the overall process */
    protected ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());

    /**
     * Constructor
     * 
     * @param hosts
     * @param workersPerHost
     * @param numberOfTasks
     */
    public OptimizationProcessor(Collection<H> hosts, int workersPerHost, Collection<R> tasks) {
        this.hosts = hosts;
        this.workersPerHost = workersPerHost;
        this.tasks = tasks;
    }

    /**
     * Processes the tasks on each of the hosts and returns once the process is
     * complete
     * 
     * @throws InterruptedException
     */
    public ListenableFuture<Duration> process() throws InterruptedException {
        return pool.submit(new ProcessingTask(LocalDateTime.now()));
    }

    @AllArgsConstructor
    protected class ProcessingTask implements Callable<Duration> {
        private LocalDateTime start;

        @Override
        public Duration call() throws Exception {
            SetMultimap<H, R> tasksForHost = buildSetMultimap();

            CountDownLatch latch = new CountDownLatch(tasksForHost.keySet().size());

            for (Map.Entry<H, Collection<R>> e : tasksForHost.asMap().entrySet()) {
                H host = e.getKey();

                ThreadFactoryBuilder b = new ThreadFactoryBuilder();
                b.setNameFormat(host + "-worker-%d");
                NamedThreadPoolExecutor executor = new NamedThreadPoolExecutor(workersPerHost, workersPerHost, 0L,
                        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), b.build());
                executor.setName(host.toString());
                executor.setLatch(latch);

                ListeningExecutorService les = MoreExecutors.listeningDecorator(executor);

                for (R task : e.getValue()) {
                    Futures.addCallback(les.submit(task), new FutureCallback<Result>() {

                        @Override
                        public void onSuccess(Result result) {
                            log.info("Success: " + result.toString());
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            log.info("Failure: " + t.getMessage());
                        }
                    });
                }
                les.shutdown();
            }
            latch.await();
            return Duration.between(start, LocalDateTime.now());
        }

    }

    /**
     * Constructs a Multimap where the key is the host and the value is a
     * collection of tasks
     * 
     * @return SetMultimap<H, T>
     */
    private SetMultimap<H, R> buildSetMultimap() {
        SetMultimap<H, R> tasksForHost;

        tasksForHost = HashMultimap.create(hosts.size(),
                (tasks.size() / workersPerHost) + (tasks.size() % workersPerHost));

        Iterator<H> hostIterator = Iterators.cycle(hosts);
        tasks.forEach(t -> tasksForHost.put(hostIterator.next(), t));

        return tasksForHost;
    }

}
