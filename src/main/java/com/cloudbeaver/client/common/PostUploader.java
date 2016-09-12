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

abstract class AbstractPostUploader implements PostUploader{
	private static final PostStringUploader postStringUploader = new PostStringUploader();
	private static final PostFileUploader postFileUploader = new PostFileUploader();

	public static final String STRING_UPLOADER = "PostStringUploader";
	public static final String BIG_FILE_UPLOADER = "PostFileUploader";

	public static final PostUploader getPostUploader(String uploaderName){
		switch (uploaderName) {
		case STRING_UPLOADER:
			return postStringUploader;

		case BIG_FILE_UPLOADER:
			return postFileUploader;

		default:
			assert(false);
		}

		return null;
	}
}

class PostStringUploader extends AbstractPostUploader{
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
        pWriter.write(startIdx > 0 ? content.substring((int)startIdx) : content);
        pWriter.flush();
	}
}

class PostFileUploader extends AbstractPostUploader{
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
	public void upload(OutputStream out, String fileName, long offset) throws IOException {
		try( RandomAccessFile in = new RandomAccessFile(fileName, "r") ){
			if (offset > 0) {
				in.seek(offset);
			}

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