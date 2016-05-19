// Copyright 2014 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.actions.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.devtools.build.lib.actions.cache.S3CacheEntry.ActionCacheEntry;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadCompatible;
import com.google.devtools.build.lib.util.Preconditions;
import com.google.devtools.build.lib.vfs.PathFragment;
import com.google.protobuf.ByteString;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.model.UploadResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

/**
 * An interface defining a cache of already-executed Actions.
 *
 * <p>This class' naming is misleading; it doesn't cache the actual actions, but it stores a
 * fingerprint of the action state (ie. a hash of the input and output files on disk), so
 * we can tell if we need to rerun an action given the state of the file system.
 *
 * <p>Each action entry uses one of its output paths as a key (after conversion
 * to the string).
 */
@ThreadCompatible
public class S3ActionCache implements ActionCache {

  private final ActionCache localCache;
  private final TransferManager transferMgr;
  private final ConcurrentHashMap<String, ActionCache.Entry> entryMap;
  private final File cacheDirectory;

  public S3ActionCache(ActionCache localCache) {
    final BasicAWSCredentials credentials = new BasicAWSCredentials(
          System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"));
    this.localCache = localCache;
    this.transferMgr = new TransferManager(credentials);
    this.entryMap = new ConcurrentHashMap<String, ActionCache.Entry>();
    this.cacheDirectory = new File(System.getProperty("user.home"), ".databricks/bazel-cache");
    this.cacheDirectory.mkdirs();
  }

  private ActionCacheEntry toProto(String outputKey, ActionCache.Entry entry) {
    ActionCacheEntry.Builder builder = ActionCacheEntry.newBuilder()
      .setOutputKey(outputKey)
      .setActionKey(entry.getActionKey())
      .setDigest(ByteString.copyFrom(entry.getFileDigest().asMetadata().digest));

    // Add an file entry for each item in the mdMap
    for (Map.Entry<String, Metadata> e : entry.getEntries()) {
      String path = e.getKey();
      ActionCacheEntry.FileEntry.Builder fileEntry = ActionCacheEntry.FileEntry.newBuilder()
        .setPath(path);

      try {
        InputStream fileIs = Files.newInputStream(new File(path).toPath());
        fileEntry.setContent(ByteString.readFrom(fileIs));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }

      // We can either have the md5 data or the last modified time.
      Metadata m = e.getValue();
      if (m.digest != null) {
        fileEntry.setDigest(ByteString.copyFrom(m.digest));
      } else {
        fileEntry.setMtime(m.mtime);
      }
      builder.addFiles(fileEntry.build());
    }

    System.out.println(builder.build());
    return builder.build();
  };

  private ActionCache.Entry fromProto(ActionCacheEntry proto) {
    ActionCache.Entry entry = this.createEntry(proto.getActionKey(), false);
    for (ActionCacheEntry.FileEntry fileEntry : proto.getFilesList()) {
      Metadata md = null;
      // TODO(ahirreddy): Not sure why oneof hasBlah() isn't working
      // if (fileEntry.hasMtime()) {
      if (fileEntry.getMtime() != 0) {
        md = new Metadata(fileEntry.getMtime());
      // } else if (fileEntry.hasDigest()) {
      } else if (fileEntry.getDigest() != ByteString.EMPTY) {
        md = new Metadata(fileEntry.getDigest().toByteArray());
      } else {
        throw new RuntimeException("ActionCacheEntry Proto missing file Metadata: " + fileEntry);
      }
      entry.addFile(new PathFragment(fileEntry.getPath()), md);
    }

    if (!Arrays.equals(entry.getFileDigest().asMetadata().digest,
          proto.getDigest().toByteArray())) {
      throw new RuntimeException("Computed digest differs from digest stored in proto");
    }

    return entry;
  };

  /**
   * Updates the cache entry for the specified key.
   */
  public void put(String key, ActionCache.Entry entry) {
    localCache.put(key, entry);
    entryMap.put(key, entry);
  };

  /**
   * Returns the corresponding cache entry for the specified key, if any, or
   * null if not found.
   */
  public ActionCache.Entry get(String key) {
    ActionCache.Entry localResult = localCache.get(key);
    if (localResult != null) {
      return localResult;
    } else {
      // TODO(ahirreddy): S3 Lookup
      return null;
    }
  };

  /**
   * Removes entry from cache
   */
  public void remove(String key) {
    // Never remove from the remote cache.
    localCache.remove(key);
  };

  /**
   * Returns a new Entry instance. This method allows ActionCache subclasses to
   * define their own Entry implementation.
   */
  public ActionCache.Entry createEntry(String key, boolean discoversInputs) {
    return localCache.createEntry(key, discoversInputs);
  };

  /**
   * Give persistent cache implementations a notification to write to disk.
   * @return size in bytes of the serialized cache.
   */
  public long save() throws IOException {
    /*
    for (Map.Entry<String, ActionCache.Entry> entry : entryMap.entrySet()) {
      String outputKey = entry.getKey();
      ActionCache.Entry actionCacheEntry = entry.getValue();
      ActionCacheEntry proto = toProto(outputKey, actionCacheEntry);
      ActionCache.Entry deserializedEntry = fromProto(proto);
      if (outputKey != proto.getOutputKey()) {
        throw new RuntimeException(
            "Proto output key differs: " + "\n\n" + outputKey + "\n\n" + proto.getOutputKey());
      }
      // TODO(ahirreddy): Is this a correct equality check?
      if (!actionCacheEntry.getActionKey().equals(deserializedEntry.getActionKey()) ||
          !actionCacheEntry.getFileDigest().equals(deserializedEntry.getFileDigest())) {
        throw new RuntimeException(
            "Proto SerDe failed: " + entry + "\n\n" + proto + "\n\n" + deserializedEntry);
      }
    }
    */

    entryMap.entrySet()
      .parallelStream()
      .filter(e ->
        !e.getValue().getEntries().stream()
          .anyMatch(entry ->
            entry.getKey().startsWith("external/") || entry.getKey().contains("_middlemen"))
      )
      .forEach(e -> {
        try {
          String outputKey = e.getKey();
          ActionCache.Entry entry = e.getValue();
          String key = entry.getActionKey() + entry.getFileDigest();
          FileOutputStream fos = new FileOutputStream(new File(this.cacheDirectory, key));
          toProto(outputKey, entry).writeDelimitedTo(fos);
          fos.flush();
          fos.close();
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      });

    return localCache.save();
  };

  /**
   * Dumps action cache content into the given PrintStream.
   */
  public void dump(PrintStream out) {
    localCache.dump(out);
  };


  /*
  private def uploadFiles(files: Seq[File], bucket: String, destPath: String): Seq[String] = {
    val uploads = files.map { file =>
      val inputStream = new FileInputStream(file)
      val metadata = new ObjectMetadata
      // Content length must be set for stream uploads.
      metadata.setContentLength(file.length())
      transferMgr.upload(bucket, Paths.get(destPath, file.getName()).toString, inputStream, metadata)
    }
    // Wait for all of the uploads to complete.
    val uploadResults: Seq[UploadResult] = uploads.map(_.waitForUploadResult())
      transferMgr.shutdownNow()
      uploadResults.map(_.getKey())
  }
  */
}
