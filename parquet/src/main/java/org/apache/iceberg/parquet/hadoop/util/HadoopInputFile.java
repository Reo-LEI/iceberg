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

package org.apache.iceberg.parquet.hadoop.util;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.Util;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class HadoopInputFile implements InputFile {

  private final FileSystem fs;
  private final FileStatus stat;
  private final Configuration conf;

  public static HadoopInputFile fromPath(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = Util.getFs(path, conf);
    return new HadoopInputFile(fs, fs.getFileStatus(path), conf);
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf)
      throws IOException {
    FileSystem fs = Util.getFs(stat.getPath(), conf);
    return new HadoopInputFile(fs, stat, conf);
  }

  private HadoopInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.stat = stat;
    this.conf = conf;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return HadoopStreams.wrap(fs.open(stat.getPath()));
  }

  @Override
  public String toString() {
    return stat.getPath().toString();
  }
}
