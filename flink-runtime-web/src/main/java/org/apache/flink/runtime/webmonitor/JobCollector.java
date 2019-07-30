package org.apache.flink.runtime.webmonitor;

import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * collect job info.
 */
public class JobCollector implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(JobCollector.class);
	private static final String JSON_FILE_ENDING = ".json";

	private Map<AppMaster, Map<String, Set<Container>>> appMasters = new ConcurrentHashMap<>();
	private YarnClient yarnClient;
	private File webDir;
	private File webJobDir;
	private File webOverviewDir;
	private ScheduledExecutorService executor;
	private ConcurrentHashMap<String, Object> tobeCanceledJobId = new ConcurrentHashMap<>();

	private static final Set<String> JOB_FINISHED_STATE =
		Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
			"FAILED",
			"CANCELED",
			"FINISHED"
		)));

	public JobCollector(File webDir) {
		YarnConfiguration yarnConfiguration = new YarnConfiguration();
		final YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(yarnConfiguration);
		yarnClient.start();
		this.yarnClient = yarnClient;
		this.webDir = checkNotNull(webDir);
		this.webJobDir = new File(webDir, "jobs");
		this.webOverviewDir = new File(webDir, "overviews");
		this.executor = Executors.newSingleThreadScheduledExecutor();
		this.executor.scheduleAtFixedRate(this, 30, 20, TimeUnit.SECONDS);
	}

	public void addAppMaster(String appId, String host, String port) {
		this.appMasters.put(new AppMaster(appId, host, port), new HashMap<>());
	}

	public void cancelJob(String jobId) {
		this.tobeCanceledJobId.put(jobId, 1);
		// do cancel
		this.executor.schedule(this, 0, TimeUnit.SECONDS);
	}

	@Override
	public void run() {
		for (AppMaster appMaster : this.appMasters.keySet().toArray(new AppMaster[0])) {
			try {
				snapshotAppMaster(appMaster);
			} catch (UnirestException e) {
				LOG.error("request for appmaster={} error", appMaster, e);
				removeAppMasterEntry(appMaster);
			} catch (Throwable e) {
				LOG.error("snapshot appmaster={} error", appMaster, e);
			}
		}
	}

	private void removeAppMasterEntry(AppMaster appMaster) {
		this.appMasters.remove(appMaster);
	}

	private void snapshotAppMaster(AppMaster appMaster) throws UnirestException, IOException, YarnException, InterruptedException {
		String baseUrl = "http://" + appMaster.host + ":" + appMaster.port;
		// job currently we  only consider one appMaster per job case
		String path = "/jobs/overview";
		JSONObject jobs = responseBodyOf(baseUrl + path).getObject();
		JSONObject job = jobs.getJSONArray("jobs").getJSONObject(0);
		String jobID = job.getString("jid");
		if (this.tobeCanceledJobId.containsKey(jobID)) {
			try {
				HttpResponse<String> response = Unirest.get(baseUrl + "/jobs/" + jobID + "/yarn-cancel")
					.asString();
				LOG.info("canceling jobId={}, response={}", jobID, response.getBody());
				// next turn to update job status
				this.executor.schedule(this, 0, TimeUnit.SECONDS);
			} finally {
				this.tobeCanceledJobId.remove(jobID);
			}
			return;
		}
		writeContent(path, jobID, jobs.toString());

		// job detail
		path = "/jobs/" + jobID;
		JSONObject jobDetail = responseBodyOf(baseUrl + path).getObject();
		writeContent(path, jobID, jobDetail.toString());

		// vertices
		path = "/jobs/" + jobID + "/vertices/details";
		JSONObject vertices = responseBodyOf(baseUrl + path).getObject();
		writeContent(path, jobID, vertices.toString());

		String jobState = job.getString("state");
		if (JOB_FINISHED_STATE.contains(jobState)) {
			removeAppMasterEntry(appMaster);
			// force appMaster to exit
			Unirest.delete(baseUrl + "/cluster").asBinary();
			Thread.sleep(1000);  // wait appMaster to exit
			try {
				// kill appMaster if it not exit
				Unirest.delete(baseUrl + "/cluster").asBinary();
				yarnClient.killApplication(appMaster.aid);
			} catch (Exception ignore) {
			}
		}

		// containersInputStream
		path = "/jobs/" + jobID + "/containers";
		JSONObject attempts = new JSONObject();
		JSONArray containers = new JSONArray();
		attempts.put("containers", containers);
		List<ApplicationAttemptReport> applicationAttempts = yarnClient.getApplicationAttempts(appMaster.aid);
		for (ApplicationAttemptReport applicationAttempt : applicationAttempts) {
			JSONObject attempt = new JSONObject();
			attempt.put("cid", applicationAttempt.getAMContainerId());
			String apptemtId = applicationAttempt.getApplicationAttemptId().toString();
			Map<String, Set<Container>> attempMap = appMasters.get(appMaster);
			if (!attempMap.containsKey(apptemtId)) {
				attempMap.put(apptemtId, new HashSet<>());
			}
			attempt.put("attemptID", apptemtId);
			attempt.put("host", applicationAttempt.getHost());
			attempt.put("trackingURL", applicationAttempt.getTrackingUrl());
			attempt.put("flinkWeb", baseUrl);
			attempt.put("state", applicationAttempt.getYarnApplicationAttemptState().name());
			List<ContainerReport> taskContainers = yarnClient.getContainers(applicationAttempt.getApplicationAttemptId());
			attempt.put("containers", new JSONArray());
			for (ContainerReport taskContainer : taskContainers) {
				Container c = new Container();
				c.cid = taskContainer.getContainerId().toString();
				c.host = taskContainer.getAssignedNode().toString();
				c.logUrl = taskContainer.getLogUrl();
				attempMap.get(apptemtId).add(c);
			}
			for (Container c : attempMap.get(apptemtId)) {
				JSONObject container = new JSONObject();
				container.put("cid", c.cid);
				container.put("node", c.host);
				container.put("logURL", c.logUrl);
				attempt.getJSONArray("containers").put(container);
			}
			containers.put(attempt);
		}
		writeContent(path, jobID, attempts.toString());

		// ommit other parts,
		// update jobs overview
		HistoryServerArchiveFetcher.updateJobOverview(webOverviewDir, webDir);
	}

	private void writeContent(String path, String jobID, String jsonStr) throws IOException {
		File target;
		if (path.equals(JobsOverviewHeaders.URL)) {
			target = new File(webOverviewDir, jobID + JSON_FILE_ENDING);
		} else {
			target = new File(webDir, path + JSON_FILE_ENDING);
		}

		java.nio.file.Path parent = target.getParentFile().toPath();

		try {
			Files.createDirectories(parent);
		} catch (FileAlreadyExistsException ignored) {
			// ianore
		}

		java.nio.file.Path targetPath = target.toPath();

		Files.deleteIfExists(targetPath);

		Files.createFile(target.toPath());
		try (FileWriter fw = new FileWriter(target)) {
			fw.write(jsonStr);
			fw.flush();
		}
	}

	private JsonNode responseBodyOf(String url) throws UnirestException {
		return Unirest.get(url)
			.asJson()
			.getBody();
	}

	static class Container {
		String cid;
		String status;
		String logUrl;
		String host;

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Container container = (Container) o;
			return cid.equals(container.cid);
		}

		@Override
		public int hashCode() {
			return Objects.hash(cid);
		}
	}

	static class AppMaster {
		String appId;
		String host;
		String port;
		ApplicationId aid;

		public AppMaster(String appId, String host, String port) {
			this.appId = appId;
			String[] t = appId.split("_");
			this.aid = ApplicationId.newInstance(Long.parseLong(t[1]), Integer.parseInt(t[2]));
			this.host = host;
			this.port = port;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			AppMaster appMaster = (AppMaster) o;
			return Objects.equals(appId, appMaster.appId) &&
				Objects.equals(host, appMaster.host) &&
				Objects.equals(port, appMaster.port);
		}

		@Override
		public int hashCode() {
			return Objects.hash(appId, host, port);
		}

		@Override
		public String toString() {
			return "AppMaster{" +
				"appId='" + appId + '\'' +
				", host='" + host + '\'' +
				", port='" + port + '\'' +
				'}';
		}
	}
}
