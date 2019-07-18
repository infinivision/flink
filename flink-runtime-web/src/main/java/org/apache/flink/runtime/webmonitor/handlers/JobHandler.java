package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.webmonitor.JobCollector;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;

import java.util.List;
import java.util.Map;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * receive new job request.
 */
@ChannelHandler.Sharable
public class JobHandler extends SimpleChannelInboundHandler<Routed> {

	private JobCollector jobCollector;

	public JobHandler(JobCollector jobCollector) {
		this.jobCollector = jobCollector;
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Routed routed) throws Exception {
		String path = routed.path();
		if (path.contains("yarn-cancel")) {
			this.jobCollector.cancelJob(routed.pathParams().get("jobid"));
		} else {
			Map<String, List<String>> queryParams = routed.queryParams();
			String appId = queryParams.get("appId").get(0);
			String host = queryParams.get("host").get(0);
			String port = queryParams.get("port").get(0);
			jobCollector.addAppMaster(appId, host, port);
		}
		HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
		ctx.write(response);
	}
}
