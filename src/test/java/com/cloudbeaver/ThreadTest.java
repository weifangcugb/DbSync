package com.cloudbeaver;

import com.cloudbeaver.client.common.BeaverUtils;

class Runner implements Runnable{
	@Override
	public void run() {
		while(true){
			System.out.println("running...");
			BeaverUtils.sleep(1000);
		}
	}
}

public class ThreadTest {
	public static void main(String[] args) throws InterruptedException {
		Thread thread = new Thread(new Runner());
		thread.start();
		thread.join(1000 * 10);
		System.out.println("main is done");
	}
}
