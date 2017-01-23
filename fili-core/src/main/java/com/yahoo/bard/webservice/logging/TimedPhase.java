// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Represents a phase that is timed.
 * TimedPhase is used to associate a Timer located in the registry with the exact duration of such a phase for a
 * specific request.
 */
public class TimedPhase implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TimedPhase.class);

    private final String name;
    private long start;
    private long duration;

    /**
     * Constructor.
     * Times are in nanoseconds.
     *
     * @param name  Name of the phase
     */
    public TimedPhase(String name) {
        this.name = name;
    }

    /**
     * Start the phase.
     *
     * @return This phase after being started
     */
    public TimedPhase start() {
        if (isStarted()) {
            LOG.warn("Tried to start timer that is already running: {}", name);
        } else {
            start = System.nanoTime();
        }
        return this;
    }

    /**
     * Stop the phase.
     */
    public void stop() {
        if (!isStarted()) {
            LOG.warn("Tried to stop timer that has not been started: {}", name);
            return;
        }
        duration += System.nanoTime() - start;
        start = 0;
    }

    public long getDuration() {
        return duration;
    }

    public String getName() {
        return name;
    }

    public TimeUnit getUnit() {
        return TimeUnit.NANOSECONDS;
    }

    public boolean isStarted() {
        return start != 0;
    }

    @Override
    public void close() {
        stop();
        RequestLog.registerTime(this);
    }
}
