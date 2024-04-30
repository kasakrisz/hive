/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestContext {
    private static HiveConf conf = new HiveConf();

    private Context context;

    @Before
    public void setUp() {
        /* Only called to create session directories used by the Context class */
        SessionState.start(conf);
        SessionState.detachSession();

        context = new Context(conf);
    }

    @Test
    public void testGetScratchDirectoriesForPaths() throws IOException {
        Context spyContext = spy(context);

        // When Object store paths are used, then getMRTmpPatch() is called to get a temporary
        // directory on the default scratch diretory location (usually /temp)
        Path mrTmpPath = new Path("hdfs://hostname/tmp/scratch");
        doReturn(mrTmpPath).when(spyContext).getMRTmpPath();
        assertEquals(mrTmpPath, spyContext.getTempDirForInterimJobPath(new Path("s3a://bucket/dir")));

        // When local filesystem paths are used, then getMRTmpPatch() should be called to
        // get a temporary directory
        assertEquals(mrTmpPath, spyContext.getTempDirForInterimJobPath(new Path("file:/user")));
        assertEquals(mrTmpPath, spyContext.getTempDirForInterimJobPath(new Path("file:///user")));

        // When Non-Object store paths are used, then getExtTmpPathRelTo is called to get a temporary
        // directory on the same path passed as a parameter
        Path tmpPathRelTo = new Path("hdfs://hostname/user");
        doReturn(tmpPathRelTo).when(spyContext).getExtTmpPathRelTo(any(Path.class));
        assertEquals(tmpPathRelTo, spyContext.getTempDirForInterimJobPath(new Path("/user")));

        conf.setBoolean(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, false);
        assertEquals(tmpPathRelTo, spyContext.getTempDirForInterimJobPath(new Path("s3a://bucket/dir")));
        assertEquals(mrTmpPath, spyContext.getTempDirForInterimJobPath(new Path("file:///user")));
        conf.setBoolean(HiveConf.ConfVars.HIVE_BLOBSTORE_OPTIMIZATIONS_ENABLED.varname, true);
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolder() {
        Context ctx = new Context(conf);
        Path path = ctx.getMRTmpPath();
        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathDoesNotCreatesTheFolderWhenMVIsLoading() {
        Context ctx = new Context(conf);
        ctx.setIsLoadingMaterializedView(true);
        Path path = ctx.getMRTmpPath();
        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathDoesNotCreatesTheFolderWhenExplain() {
        Context ctx = new Context(conf);
        ctx.setExplainConfig(new ExplainConfiguration());
        Path path = ctx.getMRTmpPath();
        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathDoesNotCreatesTheFolderWhenFalsePassed() {
        Context ctx = new Context(conf);
        Path path = ctx.getMRTmpPath(false);
        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolderWhenTruePassed() {
        Context ctx = new Context(conf);
        Path path = ctx.getMRTmpPath(true);
        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolderWhenTruePassedEvenWhenMVLoading() {
        Context ctx = new Context(conf);
        ctx.setIsLoadingMaterializedView(true);
        Path path = ctx.getMRTmpPath(true);
        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolderWhenTruePassedEvenWhenExplainPlan() {
        Context ctx = new Context(conf);
        ctx.setExplainConfig(new ExplainConfiguration());
        Path path = ctx.getMRTmpPath(true);
        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolderWhenTablePathPassed() throws URISyntaxException, IOException {
        String anyTableLocation = "anyTableLocation";
        FileUtils.deleteDirectory(new File(anyTableLocation));

        Context ctx = new Context(conf);
        Path path = ctx.getMRTmpPath(new URI(anyTableLocation));
        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolderWhenTablePathPassedAndMVIsLoading() throws URISyntaxException, IOException {
        Context ctx = new Context(conf);
        ctx.setIsLoadingMaterializedView(true);
        Path path = ctx.getMRTmpPath(new URI("anyTableLocation"));
        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testLocalGetMRTmpPathCreatesTheFolderWhenTablePathPassedAndExplainPlan() throws URISyntaxException, IOException {
        Context ctx = new Context(conf);
        ctx.setExplainConfig(new ExplainConfiguration());
        Path path = ctx.getMRTmpPath(new URI("anyTableLocation"));
        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolder() {
        Context ctx = new Context(conf);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = spyContext.getMRTmpPath();

        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathDoesNotCreatesTheFolderWhenMVIsLoading() {
        Context ctx = new Context(conf);
        ctx.setIsLoadingMaterializedView(true);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath();

        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathDoesNotCreatesTheFolderWhenExplain() {
        Context ctx = new Context(conf);
        ctx.setExplainConfig(new ExplainConfiguration());
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath();

        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathDoesNotCreatesTheFolderWhenFalsePassed() {
        Context ctx = new Context(conf);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(false);

        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolderWhenTruePassed() {
        Context ctx = new Context(conf);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(true);

        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolderWhenTruePassedEvenWhenMVLoading() {
        Context ctx = new Context(conf);
        ctx.setIsLoadingMaterializedView(true);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(true);

        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolderWhenTruePassedEvenWhenExplainPlan() {
        Context ctx = new Context(conf);
        ctx.setExplainConfig(new ExplainConfiguration());
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(true);

        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolderWhenTablePathPassed() throws URISyntaxException, IOException {
        String anyTableLocation = "anyTableLocation";
        FileUtils.deleteDirectory(new File(anyTableLocation));

        Context ctx = new Context(conf);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(new URI(anyTableLocation));

        assertTrue(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolderWhenTablePathPassedAndMVIsLoading() throws URISyntaxException, IOException {
        Context ctx = new Context(conf);
        ctx.setIsLoadingMaterializedView(true);
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(new URI("anyTableLocation"));

        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }

    @Test
    public void testGetMRTmpPathCreatesTheFolderWhenTablePathPassedAndExplainPlan() throws URISyntaxException, IOException {
        Context ctx = new Context(conf);
        ctx.setExplainConfig(new ExplainConfiguration());
        Context spyContext = spy(ctx);
        doReturn(false).when(spyContext).isLocalOnlyExecutionMode();

        Path path = ctx.getMRTmpPath(new URI("anyTableLocation"));

        assertFalse(Files.exists(Paths.get(path.getParent().toUri())));
    }
}
