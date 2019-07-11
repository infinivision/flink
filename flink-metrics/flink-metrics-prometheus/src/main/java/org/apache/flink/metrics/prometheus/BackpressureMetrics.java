package org.apache.flink.metrics.prometheus;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.prometheus.client.Gauge;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.metrics.prometheus.AbstractPrometheusReporter.CHARACTER_FILTER;

/**
 * back pressure.
 */
public class BackpressureMetrics {
	private static final Logger log = LoggerFactory.getLogger(BackpressureMetrics.class);

	private static String webMonitorAddress;

	private static String jobPath = "/jobs/overview";

	private String[] lables = new String[]{"host", "jobmanager_pid", "job_name", "vertex_name"};

	private static final String METRICS_NAME = "flink_jobvertex_back_pressure";
	private static final String HELP_STR = "HELP job vertices back pressure ";
	private String host;
	private String jobManagerPid;
	private Map<String, List<List<String>>> jobs = new HashMap<>();

	private Gauge collector;

	public static BackpressureMetrics instance;

	static {
		instance = new BackpressureMetrics();
	}

	private BackpressureMetrics() {
		Thread refreshTask = new Thread(new RefreshBackpressure());
		refreshTask.setName("refresh backpressure metrics task");
		refreshTask.start();
	}

	class RefreshBackpressure implements Runnable {

		@Override
		public void run() {
			try {
				// sleep first for job startup
				Thread.sleep(5 * 1000);

			} catch (InterruptedException e) {
				// ignore
			}
			String webMonitorAddress = System.getProperty("WEB_MONITOR_ADDRESS", null);
			if (webMonitorAddress == null) {
				log.info("web monitor address not set");
				return;
			} else {
				BackpressureMetrics.webMonitorAddress = webMonitorAddress;
				log.info("web monitor rest address = {}", webMonitorAddress);
			}
			collector = Gauge
					.build()
					.name(METRICS_NAME)
					.help(HELP_STR)
					.labelNames(lables)
					.create();
			try {
				collector.register();
			} catch (Exception e) {
				log.warn("There was a problem registering metric {}.", METRICS_NAME, e);
			}
			String pidAndHost = ManagementFactory.getRuntimeMXBean().getName();
			if (pidAndHost != null) {
				String[] split = pidAndHost.split("@");
				host = split[1];
				jobManagerPid = split[0];
			}
			while (true) {
				try {
					List<Job> allJobs = getAllJobs();
					HashMap<String, Integer> currentJobs = new HashMap<>();
					for (Job job : allJobs) {
						currentJobs.put(job.name, 1);
						getJobVertex(job);
						for (JobVertex vertex : job.vertices) {
							final Double bp = getJobVertexBackpressure(job.jid, vertex.id);
							if (bp != null) {
								log.debug("sampled backpressure={} of jobname={} vertexname={}", bp, job.name, vertex.name);
								List<String> labelValues = Arrays.asList(host, jobManagerPid, CHARACTER_FILTER.filterCharacters(job.name), CHARACTER_FILTER.filterCharacters(vertex.name));
								collector.setChild(new io.prometheus.client.Gauge.Child() {
									@Override
									public double get() {
										log.debug("push backpressure to prometheus");
										return bp;
									}
								}, labelValues.toArray(new String[labelValues.size()]));
								if (jobs.get(job.name)  == null) {
									jobs.put(job.name, new ArrayList<>());
								}
								jobs.get(job.name).add(labelValues);
							} else {
								log.debug("backpressure sameple of jobname={} vertexname={} out of time", job.name, vertex.name);
							}
						}
					}
					for (String name : jobs.keySet()) {
						if (!currentJobs.containsKey(name)) {
							log.info("delete job backpressure jobname={}", name);
							for (List<String> labelValues : jobs.get(name)) {
								collector.remove(labelValues.toArray(new String[labelValues.size()]));
							}
							jobs.remove(name);
						}
					}
					log.debug("get jobs = {}", allJobs);
					Thread.sleep(10 * 1000);
				} catch (UnirestException e) {
					log.warn("There was a problem getting all job", e);
				} catch (InterruptedException e) {
					log.info("intterrupted... ignore");
				} catch (Throwable e) {
					log.error("refresh back pressure error", e);
				}
			}
		}
	}

	static class Job {
		String jid;
		String name;
		Date startTime;
		List<JobVertex> vertices = new ArrayList<>();

		Job(String jid, String name, Date startTime) {
			this.jid = jid;
			this.name = name;
			this.startTime = startTime;
		}

		@Override
		public String toString() {
			return "Job{" +
					"jid='" + jid + '\'' +
					", name='" + name + '\'' +
					", startTime=" + startTime +
					", vertices=" + vertices +
					'}';
		}
	}

	static class JobVertex {
		String id;
		String name;

		public JobVertex(String id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public String toString() {
			return "JobVertex{" +
					"id='" + id + '\'' +
					", name='" + name + '\'' +
					'}';
		}
	}

	static List<Job> getAllJobs() throws UnirestException {
		HttpResponse<JsonNode> response = Unirest.get(webMonitorAddress + jobPath).asJson();
		if (response.getStatus() != 200) {
			return Collections.emptyList();
		}
		JsonNode body = response.getBody();
		JSONArray jobs = body.getObject().getJSONArray("jobs");
		ArrayList<Job> res = new ArrayList<>(jobs.length());
		for (int i = 0; i < jobs.length(); i++) {
			JSONObject job = jobs.getJSONObject(i);
			// filter non-running job
			if ("RUNNING".equals(job.getString("state"))) {

				res.add(
						new Job(job.getString("jid"),
								job.getString("name"),
								new Date(job.getLong("start-time"))));
			}
		}
		return res;
	}

	static void getJobVertex(Job job) throws UnirestException {
		String url = webMonitorAddress + "/jobs/" + job.jid;
		HttpResponse<JsonNode> response = Unirest.get(url).asJson();
		if (response.getStatus() != 200) {
			log.error("visite url={} error", url);
			return;
		}
		JSONArray vertices = response.getBody().getObject().getJSONArray("vertices");
		for (int i = 0; i < vertices.length(); i++) {
			JSONObject vertex = vertices.getJSONObject(i);
			// filter non-running vertex
			if ("RUNNING".equals(vertex.getString("status"))) {
				job.vertices.add(new JobVertex(vertex.getString("id"), vertex.getString("name")));
			}
		}
	}

	static Double getJobVertexBackpressure(String jid, String vid) throws UnirestException {
		String url = webMonitorAddress + "/jobs/" + jid + "/vertices/" + vid + "/backpressure";
		HttpResponse<JsonNode> response = Unirest.get(url).asJson();
		if (response.getStatus() != 200) {
			return null;
		}
		JSONObject json = response.getBody().getObject();
		if (!"ok".equals(json.getString("status"))) {
			return null;
		}
		JSONArray subtasks = json.getJSONArray("subtasks");
		double backpressure = 0;
		for (int i = 0; i < subtasks.length(); i++) {
			JSONObject subtask = subtasks.getJSONObject(i);
			double ratio = ((JSONObject) subtask).getDouble("ratio");
			if (ratio > backpressure) {
				backpressure = ratio;
			}
		}
		return backpressure;
	}
}
