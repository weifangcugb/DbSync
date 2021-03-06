package com.cloudbeaver.client.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public abstract class FixedNumThreadPool implements Runnable{
	private Logger logger = Logger.getLogger(FixedNumThreadPool.class);
	public static final String STOP_SIGNAL = "TERM";//USR2
	protected static final long HEART_BEAT_INTERVAL = 180 * 1000;
	protected static final long OTHER_THING_INTERVAL = 1200 * 1000;

    protected abstract void setup() throws BeaverFatalException;
	protected abstract void doTask(Object taskObject) throws BeaverFatalException;
	protected abstract int getThreadNum();
	protected abstract Object getTaskObject(int index);
	protected abstract long getSleepTimeBetweenTaskInnerLoop();
	protected abstract String getTaskDescription();
	protected abstract void doHeartBeat();
	protected void doOtherThings() {};

	protected void shutdown() {
//		as default, do nothing
	}

	protected void handleBeaverFatalException(BeaverFatalException e) throws Exception {
		BeaverUtils.PrintStackTrace(e);
		logger.fatal("got fatal error, exit. msg:" + e.getMessage());
	}

	private static volatile boolean KEEP_RUNNING = true;

	@Override
	public void run() {
		logger.info("start thread pool, task:" + getTaskDescription());

		Signal sig = new Signal(STOP_SIGNAL);
		Signal.handle(sig, new SignalHandler() {			
			@Override
			public void handle(Signal sig) {
				KEEP_RUNNING = false;
			}
		});

		try {
			setup();
		} catch (BeaverFatalException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("get fatal error when setup thread pool, process will exit. msg:" + e.getMessage());
			return;
		}

		ExecutorService executor = Executors.newCachedThreadPool();
		for (int i = 0; i < getThreadNum(); i++) {
			final Object taskObject = getTaskObject(i);
			/*
			 * if taskObject is null, that means subclass asks the thread-pool to skip this object
			 */
			if (taskObject == null) {
				continue;
			}

			executor.submit(new Runnable() {
				long thisThreadId = -1;
				
				@Override
				public void run() {
					thisThreadId = Thread.currentThread().getId();

					logger.info("start thread to " + getTaskDescription() + ", threadId:" + thisThreadId);
					while(KEEP_RUNNING){
						try {
							doTask(taskObject);
						} catch (BeaverFatalException e) {
							try {
								handleBeaverFatalException(e);
							} catch (Exception e1) {
								BeaverUtils.printLogExceptionAndSleep(e1, "got Exception from children class, need exit.", 1000);
								return;
							}
						} catch (Exception e){
							BeaverUtils.printLogExceptionAndSleep(e, "got Exception from children class, need exit.", 1000);
							return;
						}

						BeaverUtils.sleep(getSleepTimeBetweenTaskInnerLoop());
					}
					logger.info("exit thread to " + getTaskDescription() + ", threadId:" + thisThreadId);
				}
			});
		}

		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (isRunning()) {
					doHeartBeat();
					BeaverUtils.sleep(HEART_BEAT_INTERVAL);
				}
			}
		});

		executor.submit(new Runnable() {
			@Override
			public void run() {
				while (isRunning()) {
					doOtherThings();
					BeaverUtils.sleep(OTHER_THING_INTERVAL);
				}
			}
		});

		while (KEEP_RUNNING) {
			BeaverUtils.sleep(1 * 1000);
		}

		shutdown();
		executor.shutdown();

		while ( !executor.isTerminated() ) {
			try {
				executor.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e){
				logger.error("sleep interrupted, msg:" + e.getMessage());
			}
		}

		logger.info("start thread pool, task:" + getTaskDescription());
	}

	public static boolean isRunning(){
		return KEEP_RUNNING;
	}

	public static void main(String[] args) {

	}
}
