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
import com.cloudbeaver.server.consumer.SyncConsumer;

public class App {
	private static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {
        Options options = new Options();

        Option option = new Option("m", "module", true, "the module name you want to lautch, 'dbUploader' or 'fileUploader' or 'syncConsumer'");
        option.setRequired(true);
        options.addOption(option);

//      conf file name, useless for this version
        option = new Option("f", "conf-file", true, "specify a config file, useless in this version");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("h", "help", false, "display help text");
        option.setRequired(false);
        options.addOption(option);

        CommandLineParser parser = new GnuParser();
		try {
			CommandLine commandLine = parser.parse(options, args);
	        if (commandLine.hasOption('h')) {
	            new HelpFormatter().printHelp("java -jar dbsync ", options, true);
	            return;
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
