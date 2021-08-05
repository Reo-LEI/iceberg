/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.DESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  public static final String VERSION_HINT_FILENAME = "version-hint.text";

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final Map<String, FileSystem> CACHE = new ConcurrentHashMap<>();

  private static String HdfsAuthEnable = "true";  // enable hdfs user auth by default
  private static String HadoopUserName = getHadoopUserName() != null ? getHadoopUserName() : "Anonymous";


  private Util() {
  }

  public static void setAuthProps(boolean authEnable, String userName) {
    HdfsAuthEnable = Boolean.toString(authEnable).toLowerCase();
    HadoopUserName = userName;
  }

  private static String getHadoopUserName() {
    String user = System.getenv("HADOOP_USER_NAME");
    if (user != null && user.contains("@")) {
      user = user.split("@")[0];
    }
    return user;
  }

  public static FileSystem getFs(Path path, Configuration conf) {
    boolean authEnable = Boolean.parseBoolean(conf.get(ConfigProperties.HDFS_AUTH_ENABLE, HdfsAuthEnable));
    String userName = conf.get(ConfigProperties.HDFS_AUTH_USER, HadoopUserName);

    String proxyUserName = conf.get(ConfigProperties.ICEBERG_PROXY_USER);
    if (proxyUserName != null) {
      authEnable = true;  // enable hdfs user auth for proxy user
      userName = proxyUserName;
    }

    try {
      if (authEnable) {
        UserGroupInformation.createUserForTesting(userName, new String[]{"supergroup"});
        Path root = new Path(path.toUri().getScheme(), path.toUri().getAuthority(), "/");
        String key = userName + "@" + root;

        if (CACHE.containsKey(key)) {
          return CACHE.get(key);
        }

        String user = userName + "@" + DESUtil.encrypt(userName);
        FileSystem fs = FileSystem.get(root.toUri(), conf, user);

        CACHE.putIfAbsent(key, fs);
        LOG.info("Cache fs by key {}", key);

        return fs;
      } else {
        return path.getFileSystem(conf);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static String[] blockLocations(CombinedScanTask task, Configuration conf) {
    Set<String> locationSets = Sets.newHashSet();
    for (FileScanTask f : task.files()) {
      Path path = new Path(f.file().path().toString());
      try {
        FileSystem fs = path.getFileSystem(conf);
        for (BlockLocation b : fs.getFileBlockLocations(path, f.start(), f.length())) {
          locationSets.addAll(Arrays.asList(b.getHosts()));
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to get block locations for path {}", path, ioe);
      }
    }

    return locationSets.toArray(new String[0]);
  }

  public static String[] blockLocations(FileIO io, CombinedScanTask task) {
    Set<String> locations = Sets.newHashSet();
    for (FileScanTask f : task.files()) {
      InputFile in = io.newInputFile(f.file().path().toString());
      if (in instanceof HadoopInputFile) {
        Collections.addAll(locations, ((HadoopInputFile) in).getBlockLocations(f.start(), f.length()));
      }
    }

    return locations.toArray(HadoopInputFile.NO_LOCATION_PREFERENCE);
  }

  /**
   * From Apache Spark
   *
   * Convert URI to String.
   * Since URI.toString does not decode the uri, e.g. change '%25' to '%'.
   * Here we create a hadoop Path with the given URI, and rely on Path.toString
   * to decode the uri
   * @param uri the URI of the path
   * @return the String of the path
   */
  public static String uriToString(URI uri) {
    return new Path(uri).toString();
  }
}
