Features:
1. upload pics to flume server, then pics will send to kafka and beaver storage system
2. upload database data to flume server, too
3. get msg from kafka and upload them to beaver-web server

How to use:
usage: java -jar dbsync.jar  [-d <arg>] [-f <arg>] [-h] [-k <arg>] -m <arg>
 -d,--dir-conf <arg>        which dir will be used in FileUploader,
                            'remote': use dir set by web sync api,
                            'local': use dir set by
                            conf/SyncClient_File.properties
 -f,--conf-file <arg>       specify a config file, useless in this version
 -h,--help                  display help text
 -k,--kafka-version <arg>   which kafka version will be used, 
			    'origin':use the origin git kafka, 
                            'beaver': use the beaver authentication kafka
 -m,--module <arg>          the module name you want to lautch,
                            'dbUploader' or 
			    'fileUploader' or
                            'syncConsumer'

