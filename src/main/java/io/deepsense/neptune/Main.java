package io.deepsense.neptune;

import com.google.common.collect.Lists;
import io.deepsense.neptune.apiclient.ApiException;
import io.deepsense.neptune.apiclient.model.QueuedExperimentParams;
import io.deepsense.neptune.clientlibrary.models.Channel;
import io.deepsense.neptune.clientlibrary.models.Job;
import io.deepsense.neptune.clientlibrary.models.NeptuneContext;
import io.deepsense.neptune.clientlibrary.models.impl.context.NeptuneContextBuilderFactory;
import io.deepsense.neptune.clientlibrary.parsers.jobargumentsparser.JobArguments;
import io.deepsense.neptune.service.NeptuneService;

import java.net.URI;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by rafal on 26.02.17.
 */
public class Main {


    public static void main(String[] args) {
        final String instance = "ml.neptune.deepsense.io";

        final URI restUri = URI.create("https://" + instance);
        final URI wsUri = URI.create("ws://" + instance);


        NeptuneService neptuneService = new NeptuneService(restUri, "neptune", "xai2quigah0ooFae");
//        Experiment experiment = new Experiment("testName", "testDesc", "testProject");
        QueuedExperimentParams params = createQueuedExperimentParams("testName", "testDesc", "testProject");

        UUID createdJobId = neptuneService.createExperiment("testName", "testDesc", "testProject");
        System.out.println("Created Exp: " + createdJobId);

//        WS_API_URL = 'ws://{}:{}'.format(WS_API_HOST, WS_API_PORT)
//        System.setProperty("NEPTUNE_USER", "neptune");
//        System.setProperty("NEPTUNE_PASSWORD", "xai2quigah0ooFae");

        //mark completed
        //ping
        Runtime.getRuntime().addShutdownHook(new Thread(new ClosingThread(neptuneService, createdJobId)));

        final JobArguments jobArguments = new JobArguments(restUri, wsUri, createdJobId, false);
        try {
            NeptuneContext context = new NeptuneContextBuilderFactory().create(jobArguments).build();
            neptuneService.markJobAsExecuting(createdJobId);
            Job job = context.getJob();
            Channel<Double> channnel = job.createNumericChannel("mojkanalnumeryczny");
            for (int i = 0 ; i < 100 ; ++i) {
                channnel.send((double)i, (double)i);
                if (i % 5 == 0) {
                    neptuneService.sendPing(createdJobId);
                }
            }
//            neptuneService.markJobAsCompleted(createdJobId);
        } catch (ApiException e) {
            e.printStackTrace();
        }
//        String version = "1.4.4";
//        System.out.println(Arrays.toString(version.split(Pattern.quote("."))));
    }
    
    private static QueuedExperimentParams createQueuedExperimentParams(String name, String description, String project) {
        final QueuedExperimentParams params = new QueuedExperimentParams();
        params.setName(name);
        params.setDescription(description);
        params.setProject(project);
        params.setTags(new ArrayList<>());
        params.setParameters (new ArrayList<>());
        params.setParameterValues(new ArrayList<>());
        params.setProperties(new ArrayList<>());
        params.setRequirements(Lists.newArrayList("run-key-" + UUID.randomUUID().toString()));
        params.setDumpDirLocation("unsupported");
        params.setDumpDirRoot("unsupported");
        params.setSourceCodeLocation("unsupported");
        params.setDockerImage("unsupported");
        params.setEnqueueCommand("unsupported");
        params.setGridSearchParameters(null);
        params.setMetric(null);
        return params;
    }
}
