package com.spf.utils;

import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class HttpUtil {

	protected static RequestConfig config;

	protected static PoolingHttpClientConnectionManager connManager;

	/* 日志记录器. */
	protected static final Logger logger = LogManager.getLogger(HttpUtil.class);

	static {
		config = RequestConfig.custom() //
				// .setProxy(new HttpHost(ip, port)) //
				.setSocketTimeout(30000) // 数据传输过程中数据包之间间隔的最大时间
				.setConnectTimeout(10000) // 连接建立时间，三次握手完成时间
				.setConnectionRequestTimeout(15000).build(); // 指从连接池获取连接的超时时间

		SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
		try {
			sslContextBuilder.loadTrustMaterial(null, new TrustSelfSignedStrategy());

			SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContextBuilder.build());
			Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("https", socketFactory).register("http", new PlainConnectionSocketFactory()).build();

			connManager = new PoolingHttpClientConnectionManager( //
					socketFactoryRegistry, null, null, null, 300L, TimeUnit.SECONDS);

			connManager.setMaxTotal(64);
			connManager.setDefaultMaxPerRoute(64);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// HttpClientBuilder build = HttpClients.custom();
		// build.setProxy(new HttpHost(ip, port));

		// AuthScope authscope = new AuthScope(ip, port);
		// Credentials credentials = new UsernamePasswordCredentials("YsProxy", "YsProxy@0023");
		// CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		// credentialsProvider.setCredentials(authscope, credentials);
		// build.setDefaultCredentialsProvider(credentialsProvider);

		// CloseableHttpClient client = build.setDefaultRequestConfig(config) //
		// .setConnectionManager(connManager).build();
	}

	/**
	 * 受保护的构造方法, 防止外部构建对象实例.
	 */
	protected HttpUtil() {
		super();
	}

	/**
	 * 执行下载
	 */
	public static byte[] down(HttpClient client, final HttpUriRequest request, Logger logger) throws Exception {
		return down(client, request, null, logger);
	}

	/**
	 * 执行下载
	 */
	public static byte[] down(HttpClient client, final HttpUriRequest request, Map<String, Object> map, Logger logger)
			throws Exception {

		int status = -1;
		byte[] buffer = null;
		Header[] headers = null;
		InputStream input = null;

		HttpEntity httpEntity = null;
		HttpResponse response = null;
		ByteArrayOutputStream out = null;

		if (null == logger)
			logger = HttpUtil.logger;

		if (null == client)
			client = HttpClientBuilder.create() //
					.setDefaultRequestConfig(config) //
					.setConnectionManager(connManager).build();

		try {
			response = client.execute(request);
			httpEntity = response.getEntity();

			if (null != map) {
				status = response.getStatusLine().getStatusCode();
				headers = response.getAllHeaders();
			}

			// long length = httpEntity.getContentLength();
			// if (0L >= length) {
			// logger.warn("{}^HttpUtil.down failed, warn:{}, URL:{}", TraceUtil.tid(), "down file unexists!",
			// request.getURI().getPath());
			// return buffer;
			// }

			int size = 0;
			buffer = new byte[2048];
			input = httpEntity.getContent();

			out = new ByteArrayOutputStream();
			while ((size = input.read(buffer)) != -1)
				out.write(buffer, 0, size);

			buffer = out.toByteArray();

		} catch (Exception e) {
			logger.error("{}^HttpUtil.down failed, url:{}, {}", e.toString(), request.getURI().getPath());
			throw e;

		} finally {
			if (out != null) {
				out.close();
			}
			if (input != null) {
				input.close();
			}
			if (null != map) {
				map.put("data", buffer);
				map.put("status", status);
				if (null != headers && 0 != headers.length)
					for (Header header : headers)
						map.put(header.getName(), header.getValue());
			}
		}

		return buffer;
	}

	/**
	 * 执行HTTP请求
	 */
	public static String execute(HttpClient client, final HttpUriRequest request, Logger logger) throws Exception {
		return execute(client, request, null, logger);
	}

	/**
	 * 执行HTTP请求
	 */
	public static String execute(HttpClient client, final HttpUriRequest request, Map<String, Object> map, Logger logger)
			throws Exception {

		int status = -1;
		String data = null;
		Header[] headers = null;

		HttpEntity httpEntity = null;
		HttpResponse response = null;

		if (null == client)
			client = HttpClientBuilder.create() //
					.setDefaultRequestConfig(config).build();

		try {
			response = client.execute(request);
			httpEntity = response.getEntity();

			if (null != map) {
				status = response.getStatusLine().getStatusCode();
				headers = response.getAllHeaders();
			}

			data = EntityUtils.toString(httpEntity);
			// data = EntityUtils.toString(entity, Charset.forName("UTF-8"))

		} catch (Exception e) {
			if (null == logger)
				logger = HttpUtil.logger;
			logger.error("{}^HttpUtil.execute failed, url:{}, {}",  e.toString(), request.getURI().getPath());
			throw e;

		} finally {
			if (null != map) {
				map.put("data", data);
				map.put("status", status);
				if (null != headers && 0 != headers.length)
					for (Header header : headers)
						map.put(header.getName(), header.getValue());
			}
		}

		return data;
	}

	/**
	 *  图片上传
	 * @param client client
	 * @param httpPost post
	 * @param fileUrl 图片文件路径
	 * @param logger 日志
	 * @return object
	 */
	public static Object upLoadFile(HttpClient client,HttpPost httpPost, String fileUrl, Logger logger){
		String ret = null;
		//上传文件设置
		MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
		BufferedImage image = null;
		try {
			image = ImageIO.read(new URL(fileUrl));
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			ImageIO.write(image, "jpg", os);
			InputStream is = new ByteArrayInputStream(os.toByteArray());

			multipartEntityBuilder.addPart("imgfile",new InputStreamBody(is, "image/jpeg"));
			multipartEntityBuilder.setContentType(ContentType.MULTIPART_FORM_DATA);

			httpPost.setEntity(multipartEntityBuilder.build());
			ret = execute(client,httpPost, null, logger);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return ret;
	}

	/**
	 * 构建JOSN 类型的请求
	 * 
	 * @param url
	 * @param json
	 * @return HttpPost
	 */
	public static HttpPost jsonRequest(String url, String json) {
		HttpPost httpPost = new HttpPost(url);
		StringEntity stringEntity = new StringEntity(json, ContentType.APPLICATION_JSON);
		httpPost.setEntity(stringEntity);
		return httpPost;
	}

	/**
	 * 构建FROM 类型的请求
	 * 
	 * @param url
	 * @param inputs
	 * @return HttpPost
	 */
	public static HttpPost formRequest(String url, Map<String, ?> inputs) {
		HttpPost httpPost = new HttpPost(url);

		List<NameValuePair> pairs = new ArrayList<>();
		for (Map.Entry<String, ?> entry : inputs.entrySet())
			pairs.add(new BasicNameValuePair(entry.getKey(), String.valueOf(entry.getValue())));

		UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(pairs, Consts.UTF_8);
		httpPost.setEntity(formEntity);
		return httpPost;
	}
}
