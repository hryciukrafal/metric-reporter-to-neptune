package io.deepsense.neptune;

import io.deepsense.neptune.service.NeptuneService;

import java.util.UUID;

public class ClosingThread implements Runnable {

    private final NeptuneService neptuneService;

    private final UUID jobId;


    public ClosingThread(NeptuneService neptuneService, UUID jobId) {
        this.neptuneService = neptuneService;
        this.jobId = jobId;
    }

    @Override
    public void run() {
        System.out.println("KOnczymy, mark completed");
        neptuneService.markJobAsCompleted(jobId);
    }
}
