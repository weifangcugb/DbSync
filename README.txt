Features:
1. upload pics to flume server, then pics will send to kafka and beaver storage system
2. upload database data to flume server, too
3. get msg from kafka and upload them to beaver-web server

How to use:
usage: java -jar dbsync.jar  [-d <arg>] [-h] [-k <arg>] -m <arg>
 -d,--dir-conf <arg>        which dir will be used in FileUploader,
                            'remote': default value, use dir set by web sync api,
                            'local': use dir set by conf/SyncClient_File.properties
 -k,--kafka-version <arg>   which kafka version will be used in SyncConsumer, 
			    'origin':use the origin public kafka, 
                            'beaver': default value, use the beaver authentication kafka
 -m,--module <arg>          the module name you want to lautch,
                            'dbUploader' or 'fileUploader' or 'syncConsumer'
 -h,--help                  display help text
