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
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadCompatible;
import com.google.devtools.build.lib.util.Preconditions;
import com.google.devtools.build.lib.vfs.PathFragment;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  public S3ActionCache(ActionCache localCache) {
    this.localCache = localCache;
  }

  /**
   * Updates the cache entry for the specified key.
   */
  public void put(String key, ActionCache.Entry entry) {
    localCache.put(key, entry);
  };

  /**
   * Returns the corresponding cache entry for the specified key, if any, or
   * null if not found.
   */
  public ActionCache.Entry get(String key) {
    return localCache.get(key);
  };

  /**
   * Removes entry from cache
   */
  public void remove(String key) {
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
    return localCache.save();
  };

  /**
   * Dumps action cache content into the given PrintStream.
   */
  public void dump(PrintStream out) {
    localCache.dump(out);
  };
}
