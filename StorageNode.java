import java.io.*;
import java.util.*;

import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void main(String [] args) throws Exception {
	BasicConfigurator.configure();
	log = Logger.getLogger(StorageNode.class.getName());

	if (args.length != 4) {
	    System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
	    System.exit(-1);
	}

	CuratorFramework curClient =
	    CuratorFrameworkFactory.builder()
	    .connectString(args[2])
	    .retryPolicy(new RetryNTimes(10, 1000))
	    .connectionTimeoutMs(1000)
	    .sessionTimeoutMs(10000)
	    .build();

	curClient.start();
	Runtime.getRuntime().addShutdownHook(new Thread() {
		public void run() {
		    curClient.close();
		}
	    });

	KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<KeyValueService.Iface>(new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]));
	TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
	TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
	sargs.protocolFactory(new TBinaryProtocol.Factory());
	sargs.transportFactory(new TFramedTransport.Factory());
	sargs.processorFactory(new TProcessorFactory(processor));
	sargs.maxWorkerThreads(64);
	TServer server = new TThreadPoolServer(sargs);
	log.info("Launching server");

	new Thread(new Runnable() {
		public void run() {
		    server.serve();
		}
	    }).start();

	// initialize
		String zkNode = args[3];
		for (int i = 0; i < 2; i++) {
			curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode);
			List<String> children = curClient.getChildren().forPath(zkNode);
			if (children.size() == 0) {
				System.out.println("no children added here, please wait");
			}
		}

//		while(true) {
//			List<String> children = curClient.getChildren().forPath(zkNode);
//
//			// judge if there are nodes here
//			if (children.size() == 0) {
//				System.out.println("no children added here, please wait");
//				continue;
//			}
//
//			Collections.sort(children);
//			String primaryZkNode = children.get(0);
//			String backupZkNode = children.get(1);
//			byte[] data = curClient.getData().forPath(zkNode + "/" + primaryZkNode);
//			String strData = new String(data);
//			String[] primary = strData.split(":");
//			log.info("Current primary " + strData);
//
//			curClient.getChildren().usingWatcher(new CuratorWatcher() {
//				@Override
//				public void process(WatchedEvent watchedEvent) throws Exception {
//					switch (watchedEvent.getType()) {
//						case NodeDeleted :{
//							curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkNode + "/" + primaryZkNode);
//							curClient.setData().forPath(zkNode + "/" + primaryZkNode, data);
//						}
//					}
//				}
//			});

//			curClient.getData().watched().forPath(zkNode);
//			CuratorListener primaryListener = (curatorFramework, curatorEvent) -> {
//				// handle crash
//				try {
//					switch (curatorEvent.getType()) {
//					}
//				} catch (Exception e) {
//					log.error("Exception while processing event.", e);
//				}
//			};
//			curClient.getCuratorListenable().addListener(primaryListener);
//		}
    }
}
