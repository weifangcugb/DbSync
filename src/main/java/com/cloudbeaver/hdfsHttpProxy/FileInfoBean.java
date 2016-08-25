package com.cloudbeaver.hdfsHttpProxy;

import com.auth0.jwt.internal.com.fasterxml.jackson.annotation.JsonIgnore;

public class FileInfoBean {
	@JsonIgnore
//	public static int BUFFER_SIZE = 50;
	public static int BUFFER_SIZE = 123 * 1024;

	public int offset;
	public byte [] buffer;
	public int bufferSize;

	public FileInfoBean() {
		offset = 0;
		buffer = new byte[BUFFER_SIZE];
		bufferSize = 0;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getOffset() {
		return this.offset;
	}

	public void setBuffer(byte []buffer) {
		this.buffer  = buffer;
	}

	public byte[] getBuffer() {
		return this.buffer;
	}

	public void setBufferSize(int bufferSize){
		this.bufferSize = bufferSize;
	}

	public int getBufferSize() {
		return this.bufferSize;
	}

	public static void main(String[] args) {
	}

}
