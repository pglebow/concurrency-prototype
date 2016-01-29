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
import java.util.concurrent.Executor;
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

import lombok.AllArgsConstructor;
import lombok.NonNull;

/**
 * @author pglebow <H> The type of the host object <R> The result type of the
 *         task type T <T> The type implementing Callable<T>
 */
public class OptimizationProcessor<H extends Comparable<H>, R, T extends Callable<R>> {

    @NonNull
    protected Collection<H> hosts;

    @NonNull
    protected Collection<T> tasks;

    protected int workersPerHost;

    protected FutureCallback<R> callback;

    protected Executor callBackExecutor;

    /** Executor for the overall process */
    protected ListeningExecutorService pool;

    /**
     * Constructor
     * 
     * @param hosts
     * @param workersPerHost
     * @param numberOfTasks
     * @param callback
     *            an optional callback to be executed when each task completes
     * @param callBackExecutor
     *            an optional executor that will execute the callback
     */
    public OptimizationProcessor(Collection<H> hosts, int workersPerHost, Collection<T> tasks,
            FutureCallback<R> callback, Executor callBackExecutor) {
        this.hosts = hosts;
        this.workersPerHost = workersPerHost;
        this.tasks = tasks;
        this.callback = callback;
        this.callBackExecutor = callBackExecutor;
        this.pool = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
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
            SetMultimap<H, T> tasksForHost = buildSetMultimap();

            CountDownLatch latch = new CountDownLatch(tasksForHost.keySet().size());

            for (Map.Entry<H, Collection<T>> e : tasksForHost.asMap().entrySet()) {
                H host = e.getKey();

                ThreadFactoryBuilder b = new ThreadFactoryBuilder();
                b.setNameFormat(host + "-worker-%d");
                NamedLatchedThreadPoolExecutor executor = new NamedLatchedThreadPoolExecutor(workersPerHost,
                        workersPerHost, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), b.build());
                executor.setName(host.toString());
                executor.setLatch(latch);
                
                ListeningExecutorService les = MoreExecutors.listeningDecorator(executor);

                for (T task : e.getValue()) {
                    if (callback != null && callBackExecutor != null) {
                        Futures.addCallback(les.submit(task), callback, callBackExecutor);
                    } else {
                        les.submit(task);
                    }
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
    private SetMultimap<H, T> buildSetMultimap() {
        SetMultimap<H, T> tasksForHost;

        tasksForHost = HashMultimap.create(hosts.size(),
                (tasks.size() / workersPerHost) + (tasks.size() % workersPerHost));

        Iterator<H> hostIterator = Iterators.cycle(hosts);
        tasks.forEach(t -> tasksForHost.put(hostIterator.next(), t));

        return tasksForHost;
    }

}
