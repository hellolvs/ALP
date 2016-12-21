package lvs.zookeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import lvs.util.Constants;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class ConnectionWatcher implements Watcher{
	protected ZooKeeper zk;
	private CountDownLatch connectedSignal = new CountDownLatch(1);
	
	/**
	   * connect method
	   * @param hosts
	   *        comma separated host:port pairs, each corresponding to a zk
	   *            server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
	   *            the optional chroot suffix is used the example would look
	   *            like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
	   *            where the client would be rooted at "/app/a" and all paths
	   *            would be relative to this root - ie getting/setting/etc...
	   *            "/foo/bar" would result in operations being run on
	   *            "/app/a/foo/bar" (from the server perspective).
	   * @param sessionTimeout
	   *         session timeout in milliseconds
	   * @param watcher
	   *        a watcher object which will be notified of state changes, may
	   *            also be notified for node events
	   *
	   * @throws IOException
	   * @throws InterruptedException
	   */
	public void connect(String hosts) throws IOException, InterruptedException {
		zk = new ZooKeeper(hosts, Constants.SESSION_TIMEOUT, this);
		connectedSignal.await();
	}
	
	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}
	
	/**
	   * Close this client object. Once the client is closed, its session becomes
	   * invalid. All the ephemeral nodes in the ZooKeeper server associated with
	   * the session will be removed. The watches left on those nodes (and on
	   * their parents) will be triggered.
	   * @throws InterruptedException
	   */
	public void close() throws InterruptedException {
		zk.close();
	}

}
