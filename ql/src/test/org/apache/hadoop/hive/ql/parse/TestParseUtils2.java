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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.lenient;

@RunWith(MockitoJUnitRunner.class)
public class TestParseUtils2 {

    private HiveConf conf = new HiveConf();

    @Mock
    private IMetaStoreClient metaStoreClient;

    @Before
    public void before() throws HiveException, MetaException {
        conf.set("hive.security.authorization.manager", "");
        conf.set("hadoop.security.key.provider.path", "any");
        SessionState.start(conf);
        Hive.get(conf).setMSC(metaStoreClient);
    }

    @After
    public void after() throws Exception {
        SessionState.get().close();
    }

    @Test
    public void testParseQuery() throws Exception {
        Table table = createTableObject();

        lenient().doReturn(table).when(metaStoreClient).getTable(any(GetTableRequest.class));
        lenient().doReturn(true).when(metaStoreClient).isCompatibleWith(any(HiveConf.class));

        Context context = new Context(conf);
//        context.setIsLoadingMaterializedView(true);
        ParseUtils.parseQuery(context, "SELECT count(*) FROM a");
        // TODO: assert stage dir exists
        // something like ./ql/target/tmp/localscratchdir/215f6640-ffa4-4b3c-a30a-56a10d86df52/hive_2024-04-12_06-14-53_517_5861132994205823625-1/dummy_path
        int i = 0;
    }

    private static Table createTableObject() {
        Table table = new Table();
        StorageDescriptor storageDescriptor = new StorageDescriptor();
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setParameters(new HashMap<>());
        serdeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        storageDescriptor.setSerdeInfo(serdeInfo);
        storageDescriptor.setParameters(new HashMap<>());
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
        FieldSchema fieldSchema = new FieldSchema();
        fieldSchema.setName("col_0");
        fieldSchema.setType("int");
        storageDescriptor.setCols(Collections.singletonList(fieldSchema));
        table.setSd(storageDescriptor);
        table.setDbName("default");
        table.setTableName("a");
        table.setTableType(TableType.MANAGED_TABLE.name());
        return table;
    }
}