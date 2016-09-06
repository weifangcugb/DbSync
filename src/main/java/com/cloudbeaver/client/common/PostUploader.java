package com.cloudbeaver.client.common;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URLConnection;

import javax.net.ssl.HttpsURLConnection;

interface PostUploader {
	void setUrlConnectionProperty(URLConnection urlConnection, String content);
	void upload(OutputStream out, String content, long startIdx) throws IOException;
}

class PostStringUploader implements PostUploader{
	@Override
	public void setUrlConnectionProperty(URLConnection urlConnection, String content) {
		if (urlConnection instanceof HttpsURLConnection) {
			((HttpsURLConnection)urlConnection).setRequestProperty("Content-Length", "" + content.length());
		}else{
			((HttpURLConnection)urlConnection).setRequestProperty("Content-Length", "" + content.length());
		}
	}

	@Override
	public void upload(OutputStream out, String content, long startIdx) throws IOException {
        PrintWriter pWriter = new PrintWriter(out);
        pWriter.write(content);
        pWriter.flush();
	}
}

class PostFileUploader implements PostUploader{
	static final int READ_BUFFER_SIZE = 1024 * 10;
	static final int LOCAL_CHUNK_SIZE = 1024 * 1024;

	@Override
	public void setUrlConnectionProperty(URLConnection urlConnection, String content) {
		if (urlConnection instanceof HttpsURLConnection) {
//	      urlConnection.setUseCaches(false);
			((HttpsURLConnection)urlConnection).setChunkedStreamingMode(LOCAL_CHUNK_SIZE);
		} else {
			((HttpURLConnection)urlConnection).setChunkedStreamingMode(LOCAL_CHUNK_SIZE);
		}
	}

	@Override
	public void upload(OutputStream out, String fileName, long startIdx) throws IOException {
		try( RandomAccessFile in = new RandomAccessFile(fileName, "r") ){
			in.seek(startIdx);
			byte[] readBuf = new byte[READ_BUFFER_SIZE];
			int len = 0;
			while ((len = in.read(readBuf)) != -1) {
				out.write(readBuf, 0, len);
				out.flush();
//				BeaverUtils.sleep(100);
			}
		}
	}
}