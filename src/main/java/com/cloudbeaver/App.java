package com.cloudbeaver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.cloudbeaver.client.common.BeaverUtils;
import com.cloudbeaver.client.dbUploader.DbUploader;
import com.cloudbeaver.client.fileUploader.FileUploader;
import com.cloudbeaver.mockServer.MockWebServer;
import com.cloudbeaver.server.brokermonitor.BrokerMonitorWebServer;
import com.cloudbeaver.server.consumer.SyncConsumer;

public class App {
	private static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {
        Options options = new Options();

        Option option = new Option("m", "module", true, "the module name you want to lautch, 'dbUploader' or 'fileUploader' or 'syncConsumer' or 'mockWebServer'");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("h", "help", false, "display help text");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("k", "kafka-version", true, "which kafka version will be used,\n'origin': use the origin git kafka,\n'beaver': use the beaver authentication kafka");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("d", "dir-conf", true, "which dir will be used in FileUploader,\n'remote': use dir set by web sync api,\n'local': use dir set by conf/SyncClient_File.properties");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("l", "large file size", true, "set how large a file is large file,\n should be larger than 100K");

        option.setRequired(false);
        options.addOption(option);

        CommandLineParser parser = new GnuParser();
		try {
			CommandLine commandLine = parser.parse(options, args);
	        if (commandLine.hasOption('h')) {
	            new HelpFormatter().printHelp("java -jar dbsync ", options, true);
	            return;
            }

            if (commandLine.hasOption('l')) {
            	String largeFileSize = commandLine.getOptionValue('l');
				FileUploader.LARGE_PIC_SIZE_BARRIER = Integer.parseInt(largeFileSize);
				if (FileUploader.LARGE_PIC_SIZE_BARRIER <= 102400) {
					throw new ParseException("large file size should be larger than 102400");
				}
			}

            if (commandLine.hasOption('k')) {
            	String kafkaVersion = commandLine.getOptionValue('k');
            	switch (kafkaVersion) {
					case "beaver":
						SyncConsumer.USE_BEAVER_KAFKA = true;
						break;
					case "origin":
						SyncConsumer.USE_BEAVER_KAFKA = false;
						break;
					default:
						throw new ParseException("kafka version is wrong, only 'beaver' or 'origin' allowed");
				}
			}

            if (commandLine.hasOption('d')) {
            	String dirLocation = commandLine.getOptionValue('d');
            	switch (dirLocation) {
					case "local":
						FileUploader.USE_REMOTE_DIRS = false;
						break;
					case "remote":
						FileUploader.USE_REMOTE_DIRS = true;
						break;
					default:
						throw new ParseException("dir location is wrong, only 'local' or 'remote' allowed.");
				}
			}

            if (commandLine.hasOption('m')) {
            	String moduleName = commandLine.getOptionValue('m');
                switch (moduleName) {
                    case "dbUploader":
                    	DbUploader.startDbUploader();
                    	break;
                    case "fileUploader":
                    	FileUploader.startFileUploader();
                    	break;
                    case "syncConsumer":
                    	SyncConsumer.startSyncConsumer();
                    	break;
                    case "brokerMonitor":
                    	BrokerMonitorWebServer.startBrokerMonitor();
                    	break;
                    case "mockWebServer":
                    	MockWebServer.startMockWebServer();
                    	break;
                    default:
                    	throw new ParseException("module name is wrong");
                }
	  		}
		} catch (ParseException e) {
			BeaverUtils.PrintStackTrace(e);
			logger.fatal("command line is wrong, msg:" + e.getMessage());
			new HelpFormatter().printHelp("java -jar dbsync ", options, true);
		}
    }
}
