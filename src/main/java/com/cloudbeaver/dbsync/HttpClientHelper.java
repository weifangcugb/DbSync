package com.cloudbeaver.dbsync;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by gaobin on 16-4-8.
 */
class HttpClientHelper {

    private static Logger logger = Logger.getLogger(HttpClientHelper.class);

    private HttpClientHelper() {}
    private static HttpClientBuilder hcb = HttpClientBuilder.create();
    private static HttpClient httpClient = hcb.build();

    private static HttpRequest makeRequest(String method, String uri, Object params) {
        HttpRequest httpRequest = null;
        if (method.equalsIgnoreCase("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equalsIgnoreCase("POST")) {
            HttpPost httpPost = new HttpPost(uri);
            if (params != null) {
                if (params instanceof String) {
                    String paramString = (String) params;
                    if (paramString.length() > 0) {
                        httpPost.setEntity(new StringEntity(paramString, HTTP.UTF_8));
                    }
                } else if (params instanceof Map) {
                    List<NameValuePair> paramList = new ArrayList<NameValuePair>();
                    Map<String, String> paramMap = (Map<String, String>) params;
                    if (paramMap != null && paramMap.size() > 0) {
                        for (String k : paramMap.keySet()) {
                            paramList.add(new BasicNameValuePair(k, paramMap.get(k)));
                        }
                    }
                    try {
                        httpPost.setEntity(new UrlEncodedFormEntity(paramList, HTTP.UTF_8));
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        logger.error("Invalid params : " + e.getMessage());
                    }
                }
            }
            httpRequest = httpPost;
        } else {
            httpRequest = new BasicHttpRequest(method, uri);
        }
        return httpRequest;
    }

    public static String request(String method, String host, int port, String uri, Map<String, String> params) {
        return _request(method, host, port, uri, params);
    }

    public static String request(String method, String host, int port, String uri, String params) {
        return _request(method, host, port, uri, params);
    }

    private static String _request(String method, String host, int port, String uri, Object params) {
        String responseBody = "";
        StringBuilder sb = new StringBuilder();
        HttpRequest httpRequest = makeRequest(method, uri, params);
        try {
            HttpResponse response = httpClient.execute(new HttpHost(host, port), httpRequest);
            int statusCode = response.getStatusLine().getStatusCode();
            logger.debug("http response status code : " + statusCode);
            if (statusCode == HttpStatus.SC_OK) {
                InputStream content = response.getEntity().getContent();
                byte[] bytes = new byte[65536];
                int len;
                while ((len = response.getEntity().getContent().read(bytes, 0, 65536)) > 0) {
                    sb.append(new String(bytes, 0, len));
                }
                responseBody = sb.toString();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return responseBody;
    }

    public static String request(String method, String urlSpec) {
        return _request(method, urlSpec, null);
    }

    public static String request(String method, String urlSpec, Map<String, String> params) {
        return _request(method, urlSpec, params);
    }

    public static String request(String method, String urlSpec, String params) {
        return _request(method, urlSpec, params);
    }

    private static String _request(String method, String urlSpec, Object params) {
        if (urlSpec != null && !urlSpec.contains("://")) {
            urlSpec = "http://" + urlSpec;
        }
        String uri = "/";
        String host = "localhost";
        int port = 80;
        try {
            URL url = new URL(urlSpec);
            host = url.getHost();
            port = url.getPort();
            uri = url.getFile();
            if (host == null || host.length() == 0) {
                host = "localhost";
            }
            if (port == 0) {
                port = 80;
            }
            if (uri == null || uri.length() == 0) {
                uri = "/";
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return _request(method, host, port, uri, params);
    }

    public static String get(String url) {
        return request("GET", url);
    }

    public static String post(String url, Map<String, String> params) {
        return request("POST", url, params);
    }

    public static String post(String url, String params) {
        return request("POST", url, params);
    }

    public static String post(String url) {
        return post (url, "");
    }
}
