package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestParseUtils2 {

  HiveConf conf = new HiveConf();

  @Before
  public void before() {
    conf.set("hive.security.authorization.manager", "");
    conf.set("hadoop.security.key.provider.path", "any");
    SessionState.start((HiveConf) conf);
  }

  @After
  public void after() throws Exception {
    SessionState.get().close();
  }

  @Test
  public void testParseQuery() throws ParseException, SemanticException {
    ParseUtils.parseQuery(conf, "select 1");
    // TODO: assert stage dir exists
    // something like ./ql/target/tmp/localscratchdir/215f6640-ffa4-4b3c-a30a-56a10d86df52/hive_2024-04-12_06-14-53_517_5861132994205823625-1/dummy_path
  }
}