package io.deepsense.neptune.service;

import com.google.common.collect.Lists;
import io.deepsense.neptune.apiclient.ApiClient;
import io.deepsense.neptune.apiclient.ApiException;
import io.deepsense.neptune.apiclient.api.DefaultApi;
import io.deepsense.neptune.apiclient.auth.HttpBasicAuth;
import io.deepsense.neptune.apiclient.model.CompletedJobParams;
import io.deepsense.neptune.apiclient.model.ExecutingJobParams;
import io.deepsense.neptune.apiclient.model.JobState;
import io.deepsense.neptune.apiclient.model.QueuedExperimentParams;

import java.net.URI;
import java.util.*;

public class NeptuneService {

    private static final String UNSUPPORTED = "unsupported";

    private final DefaultApi defaultApi;


    public NeptuneService(URI url, String user, String password) {
        this.defaultApi = new DefaultApi(createApiClient(url.toString(), user, password));
    }

    public UUID createExperiment(String name, String description, String project) {
        final QueuedExperimentParams params = createQueuedExperimentParams(name, description, project);
        try {
            return UUID.fromString(defaultApi.createExperiment(params, null).getBestJob().getId());
        } catch (ApiException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void markJobAsExecuting(UUID jobId) {
        final ExecutingJobParams params = createExecutingJobParams();
        try {
            defaultApi.jobsJobIdMarkExecutingPost(jobId.toString(), params);
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    public void markJobAsCompleted(UUID jobId) {
        final CompletedJobParams params = createCompletedJobParams();
        try {
            defaultApi.jobsJobIdMarkCompletedPost(jobId.toString(), params);
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    private CompletedJobParams createCompletedJobParams() {
        final CompletedJobParams params = new CompletedJobParams();
        params.setState(JobState.SUCCEEDED);
        params.setTraceback("");
        return params;
    }

    public void sendPing(UUID jobId) {
        try {
            defaultApi.jobsJobIdPingPost(jobId.toString());
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    private QueuedExperimentParams createQueuedExperimentParams(String name, String description, String project) {
        final QueuedExperimentParams params = new QueuedExperimentParams();
        params.setName(name);
        params.setDescription(description);
        params.setProject(project);
        params.setTags(new ArrayList<>());
        params.setParameters (new ArrayList<>());
        params.setParameterValues(new ArrayList<>());
        params.setProperties(new ArrayList<>());
        params.setRequirements(Lists.newArrayList("run-key-" + UUID.randomUUID().toString()));
        params.setDumpDirLocation(UNSUPPORTED);
        params.setDumpDirRoot(UNSUPPORTED);
        params.setSourceCodeLocation(UNSUPPORTED);
        params.setDockerImage(UNSUPPORTED);
        params.setEnqueueCommand(UNSUPPORTED);
        params.setGridSearchParameters(null);
        params.setMetric(null);
        return params;
    }

    private ExecutingJobParams createExecutingJobParams() {
        final ExecutingJobParams params = new ExecutingJobParams();
        params.setDumpDirLocation(UNSUPPORTED);
        params.setSourceCodeLocation(UNSUPPORTED);
        params.setStdoutLogLocation(UNSUPPORTED);
        params.stderrLogLocation(UNSUPPORTED);
        params.setRunCommand(UNSUPPORTED);
        params.setDockerImage(UNSUPPORTED);
        params.setParameterValues(new ArrayList<>());
        return params;
    }

    private ApiClient createApiClient(String baseUrl, String username, String password) {
        ApiClient client = new ApiClient();
        client.setBasePath(baseUrl);
        addAuthenticationHeader(client, username, password);
        return client;
    }

    private void addAuthenticationHeader(ApiClient apiClient, String username, String password) {
        HttpBasicAuth httpBasicAuth = new HttpBasicAuth();
        httpBasicAuth.setUsername(username);
        httpBasicAuth.setPassword(password);
        Map<String, String> headers = new HashMap<>();
        httpBasicAuth.applyToParams(Collections.emptyList(), headers);
        Map.Entry<String, String> authenticationHeader = headers.entrySet().iterator().next();
        apiClient.addDefaultHeader(authenticationHeader.getKey(), authenticationHeader.getValue());
    }
}
