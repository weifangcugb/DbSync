package com.cloudbeaver.dbsync;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
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

    private static HttpRequest makeRequest(String method, String uri, Map<String, String> params) {
        HttpRequest httpRequest = null;
        if (method.equalsIgnoreCase("GET")) {
            httpRequest = new HttpGet(uri);
        } else if (method.equalsIgnoreCase("POST")) {
            HttpPost httpPost = new HttpPost(uri);
            List<NameValuePair> paramList = new ArrayList<NameValuePair>();
            if (params != null && params.size() > 0) {
                for (String k : params.keySet()) {
                    paramList.add(new BasicNameValuePair(k, params.get(k)));
                }
            }
            try {
                httpPost.setEntity(new UrlEncodedFormEntity(paramList, HTTP.UTF_8));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                logger.error("Invalid params : " + e.getMessage());
            }
            httpRequest = httpPost;
        } else {
            httpRequest = new BasicHttpRequest(method, uri);
        }
        return httpRequest;
    }

    public static String request(String method, String host, int port, String uri, Map<String, String> params) {
        String responseBody = "";
        HttpRequest httpRequest = makeRequest(method, uri, params);
        try {
            HttpResponse response = httpClient.execute(new HttpHost(host, port), httpRequest);
            byte[] bytes = new byte[100000];
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                response.getEntity().getContent().read(bytes);
                responseBody = new String(bytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return responseBody;
    }

    public static String request(String method, String url, Map<String, String> params) {
        String uri;
        String host;
        int port;
        int indexOfDoubleBackSlash = url.indexOf("://");
        if (indexOfDoubleBackSlash >= 0) {
            url = url.substring(indexOfDoubleBackSlash + 3);
        }
        int indexOfBackSlash = url.indexOf('/');
        String hostAndPort;
        if (indexOfBackSlash > 0) {
            hostAndPort = url.substring(0, indexOfBackSlash);
            uri = url.substring(indexOfBackSlash);
        } else if (indexOfBackSlash == 0) {
            hostAndPort = "localhost:80";
            uri = url;
        } else {
            hostAndPort = url;
            uri = "/";
        }
        int indexOfColon = hostAndPort.indexOf(':');
        if (indexOfColon > 0) {
            host = hostAndPort.substring(0, indexOfColon);
            port = Integer.parseInt(hostAndPort.substring(indexOfColon + 1));
        } else if (indexOfColon == 0) {
            host = "localhost";
            port = Integer.parseInt(hostAndPort.substring(indexOfColon + 1));
        } else {
            host = hostAndPort;
            port = 80;
        }
        return request(method, host, port, uri, params);
    }

    public static String get(String url) {
        return request("GET", url, null);
    }

    public static String post(String url, Map<String, String> params) {
        return request("POST", url, params);
    }

    public static String post(String url) {
        return request("POST", url, null);
    }
}
