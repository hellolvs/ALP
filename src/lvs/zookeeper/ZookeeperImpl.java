
package lvs.zookeeper;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.KeeperException;

/**
 *
 * ZookeeperImpl A concrete class that encapsulates Zookeeper.
 *
 */
public class ZookeeperImpl extends ConnectionWatcher{
  /** State a Zookeeper type of variable zk */
  //private ZooKeeper zk = null;


  /**
   * constructor
   * @param connectString
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
   
  public ZookeeperImpl(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    zk = new ZooKeeper(connectString, sessionTimeout, watcher);
  }
  */


  /**
   * getter method
   * @return
   *        zk
   */
  public ZooKeeper getZk() {
    return zk;
  }

  /**
   * Return the stat of the node of the given path. Return null if no such a
   * node exists.
   * @param path
   *        the node path
   * @param watch
   *        whether need to watch this node
   * @return
   *        the stat of the node of the given path; return null if no such a
   *         node exists.
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat exists(String path, boolean watch) throws KeeperException,
      InterruptedException {
    return zk.exists(path, watch);
  }

  /**
   * Create an ephemeral node with the given path.
   * @param path
   *        the path for the node
   * @param data
   *        the initial data for the node
   * @return
   *         the actual path of the created node
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String createEphemeral(String path, byte[] data) throws KeeperException, InterruptedException {
    return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }
  
  /**
   * Create a persistent node with the given path.
   * @param path
   *        the path for the node
   * @param data
   *        the initial data for the node
   * @return
   *         the actual path of the created node
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String createPersistent(String path, byte[] data) throws KeeperException, InterruptedException {
    return zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
  }

  /**
   * Set the data for the node of the given path if such a node exists and the
   * given version matches the version of the node (if the given version is
   * -1, it matches any node's versions). Return the stat of the node.
   * @param path
   *        the path of the node
   * @param data
   *        the data to set
   * @param version
   *         the expected matching version
   * @return
   *         the state of the node
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat setData(String path, byte[] data, int version)
      throws KeeperException, InterruptedException {
    return zk.setData(path, data, version);
  }

  /**
   * Determine stat whether is null.
   * @param leader
   *       the node path
   * @param b
   *         whether need to watch this node
   * @return
   *         if equal return true else false.
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean equaltostat(String leader, boolean b) throws KeeperException,
      InterruptedException {
    Stat s = null;
    s = zk.exists(leader, b);
    if (s == null) {
      return true;
    }
    return false;
  }
  
  /**
   * Determine stat whether is not null.
   * @param leader
   *        the node path
   * @param b
   *        whether need to watch this node
   * @return
   *        if not equal return true else false.
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean equaltoStat(String leader, boolean b) throws KeeperException,
  	  InterruptedException {
	Stat s = null;
	s = exists(leader, b);
	if (s != null) {
		return true;
	}
	return false;
  }

  /**
   * Determine state whether is equal.
   * @return
   *       if equal return true else false.
   */
  public boolean equaltoState() {
    return States.CONNECTING == zk.getState();
  }

  /**
   * Return the data and the stat of the node of the given path.
   * @param path
   *        the given path
   * @param watch
   *        whether need to watch this node
   * @param stat
   *         the stat of the node
   * @return
   *        the data of the node
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getData(String path, boolean watch)
      throws KeeperException, InterruptedException {
    return zk.getData(path, watch, zk.exists(path, false));
  }

  /**
   * Return the list of the children of the node of the given path.
   * @param path
   *        the given path
   * @param watch
   *        whether need to watch this node
   * @return
   *        an unordered array of children of the node with the given path
   * @throws KeeperException
   * @throws InterruptedException
   */
  public List<String> getChildren(String path, boolean watch)
      throws KeeperException, InterruptedException {
    return zk.getChildren(path, watch);
  }

  /**
   * Delete the node with the given path. The call will succeed if such a node
   * exists, and the given version matches the node's version (if the given
   * version is -1, it matches any node's versions).
   * @param path
   *        the path of the node to be deleted.
   * @param version
   *        the expected node version.
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void delete(String path, int version) throws InterruptedException,
      KeeperException {
    zk.delete(path, version);
  }

}
