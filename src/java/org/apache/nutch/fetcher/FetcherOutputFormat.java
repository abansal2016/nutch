/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.fetcher;

import java.io.IOException;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseOutputFormat;
import org.apache.nutch.protocol.Content;

import org.json.simple.JSONObject;
import com.hadoop.compression.lzo.LzopCodec;

/** Splits FetcherOutput entries into multiple map files. */
public class FetcherOutputFormat implements OutputFormat<Text, NutchWritable> {

  public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
    Path out = FileOutputFormat.getOutputPath(job);
    if ((out == null) && (job.getNumReduceTasks() != 0)) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    if (fs == null) {
      fs = out.getFileSystem(job);
    }
    if (fs.exists(new Path(out, CrawlDatum.FETCH_DIR_NAME)))
      throw new IOException("Segment already fetched!");
  }

  public RecordWriter<Text, NutchWritable> getRecordWriter(final FileSystem fs,
      final JobConf job, final String name, final Progressable progress)
          throws IOException {

    Path out = FileOutputFormat.getOutputPath(job);
    final Path fetch = new Path(new Path(out, CrawlDatum.FETCH_DIR_NAME), name);
    final Path content = new Path(new Path(out, Content.DIR_NAME), name);

    final CompressionType compType = SequenceFileOutputFormat
        .getOutputCompressionType(job);

    Option fKeyClassOpt = MapFile.Writer.keyClass(Text.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option fValClassOpt = SequenceFile.Writer.valueClass(CrawlDatum.class);
    org.apache.hadoop.io.SequenceFile.Writer.Option fProgressOpt = SequenceFile.Writer.progressable(progress);
    org.apache.hadoop.io.SequenceFile.Writer.Option fCompOpt = SequenceFile.Writer.compression(compType);

    final MapFile.Writer fetchOut = new MapFile.Writer(job,
        fetch, fKeyClassOpt, fValClassOpt, fCompOpt, fProgressOpt);

    return new RecordWriter<Text, NutchWritable>() {
      private RecordWriter<Text, NullWritable> contentOut;
      private RecordWriter<Text, Parse> parseOut;

      {
        if (Fetcher.isStoringContent(job)) {
          TextOutputFormat txtout = new TextOutputFormat();
          JobConf job_new = new JobConf(job);
          txtout.setOutputPath(job, new Path(out, Content.DIR_NAME));
          txtout.setCompressOutput(job, true);
          txtout.setOutputCompressorClass(job, LzopCodec.class);
          contentOut = txtout.getRecordWriter(fs, job, name, progress);
        }

        if (Fetcher.isParsing(job)) {
          parseOut = new ParseOutputFormat().getRecordWriter(fs, job, name,
              progress);
        }
      }

      public void write(Text key, NutchWritable value) throws IOException {

        Writable w = value.get();

        if (w instanceof CrawlDatum)
          fetchOut.append(key, w);
        else if (w instanceof Content && contentOut != null) {
          JSONObject obj = new JSONObject();
          String html_content = new String(((Content) w).getContent());
          obj.put("url", key.toString());
          obj.put("url_content", html_content);
          Text new_key = new Text(obj.toString());
          contentOut.write(new_key, NullWritable.get());
          //contentOut.write(key, (Content) w);
        }
        else if (w instanceof Parse && parseOut != null)
          parseOut.write(key, (Parse) w);
      }

      public void close(Reporter reporter) throws IOException {
        fetchOut.close();
        if (contentOut != null) {
          contentOut.close(reporter);
        }
        if (parseOut != null) {
          parseOut.close(reporter);
        }
      }

    };

  }
}
