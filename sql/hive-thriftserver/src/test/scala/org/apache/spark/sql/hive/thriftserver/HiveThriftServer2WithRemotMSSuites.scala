/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver

import java.io.{File, FileInputStream}
import java.net.{InetAddress, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.security.{KeyStore, PrivilegedAction}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.{Locale, Properties}

import com.google.common.io.Files
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{MiniDFSCluster, MiniDFSClusterWithNodeGroup}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.HiveMetaStore
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.jdbc.{HiveConnection, HiveDriver}
import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.sql.test.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.util.{ThreadUtils, Utils}
import org.codehaus.mojo.keytool.requests.KeyToolGenerateCertificateRequest
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.{Random, Try}


//class HiveThriftBinaryServerForProxySuite extends HiveThriftJdbcForProxyTest {
//  override def mode: ServerMode.Value = ServerMode.binary
//
//  private def withCLIServiceClient(f: ThriftCLIServiceClient => Unit): Unit = {
//    // Transport creation logic below mimics HiveConnection.createBinaryTransport
//    val rawTransport = new TSocket("localhost", serverPort)
//    val user = System.getProperty("user.name")
//    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
//    val protocol = new TBinaryProtocol(transport)
//    val client = new ThriftCLIServiceClient(new ThriftserverShimUtils.Client(protocol))
//
//    transport.open()
//    try f(client) finally transport.close()
//  }
//
//  test("GetInfo Thrift API") {
//    withCLIServiceClient { client =>
//      val user = System.getProperty("user.name")
//      val sessionHandle = client.openSession(user, "")
//
//      assertResult("Spark SQL", "Wrong GetInfo(CLI_DBMS_NAME) result") {
//        client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME).getStringValue
//      }
//
//      assertResult("Spark SQL", "Wrong GetInfo(CLI_SERVER_NAME) result") {
//        client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME).getStringValue
//      }
//
//      assertResult(true, "Spark version shouldn't be \"Unknown\"") {
//        val version = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER).getStringValue
//        logInfo(s"Spark version: $version")
//        version != "Unknown"
//      }
//    }
//  }
//
//  test("SPARK-16563 ThriftCLIService FetchResults repeat fetching result") {
//    withCLIServiceClient { client =>
//      val user = System.getProperty("user.name")
//      val sessionHandle = client.openSession(user, "")
//
//      withJdbcStatement("test_16563") { statement =>
//        val queries = Seq(
//          "CREATE TABLE test_16563(key INT, val STRING)",
//          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_16563")
//
//        queries.foreach(statement.execute)
//        val confOverlay = new java.util.HashMap[java.lang.String, java.lang.String]
//        val operationHandle = client.executeStatement(
//          sessionHandle,
//          "SELECT * FROM test_16563",
//          confOverlay)
//
//        // Fetch result first time
//        assertResult(5, "Fetching result first time from next row") {
//
//          val rows_next = client.fetchResults(
//            operationHandle,
//            FetchOrientation.FETCH_NEXT,
//            1000,
//            FetchType.QUERY_OUTPUT)
//
//          rows_next.numRows()
//        }
//
//        // Fetch result second time from first row
//        assertResult(5, "Repeat fetching result from first row") {
//
//          val rows_first = client.fetchResults(
//            operationHandle,
//            FetchOrientation.FETCH_FIRST,
//            1000,
//            FetchType.QUERY_OUTPUT)
//
//          rows_first.numRows()
//        }
//      }
//    }
//  }
//
//  test("Support beeline --hiveconf and --hivevar") {
//    withJdbcStatement() { statement =>
//      executeTest(hiveConfList)
//      executeTest(hiveVarList)
//      def executeTest(hiveList: String): Unit = {
//        hiveList.split(";").foreach{ m =>
//          val kv = m.split("=")
//          // select "${a}"; ---> avalue
//          val resultSet = statement.executeQuery("select \"${" + kv(0) + "}\"")
//          resultSet.next()
//          assert(resultSet.getString(1) === kv(1))
//        }
//      }
//    }
//  }
//
//  test("JDBC query execution") {
//    withJdbcStatement("test") { statement =>
//      val queries = Seq(
//        "SET spark.sql.shuffle.partitions=3",
//        "CREATE TABLE test(key INT, val STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
//        "CACHE TABLE test")
//
//      queries.foreach(statement.execute)
//
//      assertResult(5, "Row count mismatch") {
//        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
//        resultSet.next()
//        resultSet.getInt(1)
//      }
//    }
//  }
//
//  test("Checks Hive version") {
//    withJdbcStatement() { statement =>
//      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
//      resultSet.next()
//      assert(resultSet.getString(1) === "spark.sql.hive.version")
//      assert(resultSet.getString(2) === HiveUtils.builtinHiveVersion)
//    }
//  }
//
//  test("SPARK-3004 regression: result set containing NULL") {
//    withJdbcStatement("test_null") { statement =>
//      val queries = Seq(
//        "CREATE TABLE test_null(key INT, val STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKvWithNull}' OVERWRITE INTO TABLE test_null")
//
//      queries.foreach(statement.execute)
//
//      val resultSet = statement.executeQuery("SELECT * FROM test_null WHERE key IS NULL")
//
//      (0 until 5).foreach { _ =>
//        resultSet.next()
//        assert(resultSet.getInt(1) === 0)
//        assert(resultSet.wasNull())
//      }
//
//      assert(!resultSet.next())
//    }
//  }
//
//  test("SPARK-4292 regression: result set iterator issue") {
//    withJdbcStatement("test_4292") { statement =>
//      val queries = Seq(
//        "CREATE TABLE test_4292(key INT, val STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_4292")
//
//      queries.foreach(statement.execute)
//
//      val resultSet = statement.executeQuery("SELECT key FROM test_4292")
//
//      Seq(238, 86, 311, 27, 165).foreach { key =>
//        resultSet.next()
//        assert(resultSet.getInt(1) === key)
//      }
//    }
//  }
//
//  test("SPARK-4309 regression: Date type support") {
//    withJdbcStatement("test_date") { statement =>
//      val queries = Seq(
//        "CREATE TABLE test_date(key INT, value STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_date")
//
//      queries.foreach(statement.execute)
//
//      assertResult(Date.valueOf("2011-01-01")) {
//        val resultSet = statement.executeQuery(
//          "SELECT CAST('2011-01-01' as date) FROM test_date LIMIT 1")
//        resultSet.next()
//        resultSet.getDate(1)
//      }
//    }
//  }
//
//  test("SPARK-4407 regression: Complex type support") {
//    withJdbcStatement("test_map") { statement =>
//      val queries = Seq(
//        "CREATE TABLE test_map(key INT, value STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")
//
//      queries.foreach(statement.execute)
//
//      assertResult("""{238:"val_238"}""") {
//        val resultSet = statement.executeQuery("SELECT MAP(key, value) FROM test_map LIMIT 1")
//        resultSet.next()
//        resultSet.getString(1)
//      }
//
//      assertResult("""["238","val_238"]""") {
//        val resultSet = statement.executeQuery(
//          "SELECT ARRAY(CAST(key AS STRING), value) FROM test_map LIMIT 1")
//        resultSet.next()
//        resultSet.getString(1)
//      }
//    }
//  }
//
//  test("SPARK-12143 regression: Binary type support") {
//    withJdbcStatement("test_binary") { statement =>
//      val queries = Seq(
//        "CREATE TABLE test_binary(key INT, value STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_binary")
//
//      queries.foreach(statement.execute)
//
//      val expected: Array[Byte] = "val_238".getBytes
//      assertResult(expected) {
//        val resultSet = statement.executeQuery(
//          "SELECT CAST(value as BINARY) FROM test_binary LIMIT 1")
//        resultSet.next()
//        resultSet.getObject(1)
//      }
//    }
//  }
//
//  test("test multiple session") {
//    import org.apache.spark.sql.internal.SQLConf
//    var defaultV1: String = null
//    var defaultV2: String = null
//    var data: ArrayBuffer[Int] = null
//
//    withMultipleConnectionJdbcStatement("test_map", "db1.test_map2")(
//      // create table
//      { statement =>
//
//        val queries = Seq(
//            "CREATE TABLE test_map(key INT, value STRING)",
//            s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map",
//            "CACHE TABLE test_table AS SELECT key FROM test_map ORDER BY key DESC",
//            "CREATE DATABASE db1")
//
//        queries.foreach(statement.execute)
//
//        val plan = statement.executeQuery("explain select * from test_table")
//        plan.next()
//        plan.next()
//        assert(plan.getString(1).contains("Scan In-memory table `test_table`"))
//
//        val rs1 = statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
//        val buf1 = new collection.mutable.ArrayBuffer[Int]()
//        while (rs1.next()) {
//          buf1 += rs1.getInt(1)
//        }
//        rs1.close()
//
//        val rs2 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
//        val buf2 = new collection.mutable.ArrayBuffer[Int]()
//        while (rs2.next()) {
//          buf2 += rs2.getInt(1)
//        }
//        rs2.close()
//
//        assert(buf1 === buf2)
//
//        data = buf1
//      },
//
//      // first session, we get the default value of the session status
//      { statement =>
//
//        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
//        rs1.next()
//        defaultV1 = rs1.getString(1)
//        assert(defaultV1 != "200")
//        rs1.close()
//
//        val rs2 = statement.executeQuery("SET hive.cli.print.header")
//        rs2.next()
//
//        defaultV2 = rs2.getString(1)
//        assert(defaultV1 != "true")
//        rs2.close()
//      },
//
//      // second session, we update the session status
//      { statement =>
//
//        val queries = Seq(
//            s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}=291",
//            "SET hive.cli.print.header=true"
//            )
//
//        queries.map(statement.execute)
//        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
//        rs1.next()
//        assert("spark.sql.shuffle.partitions" === rs1.getString(1))
//        assert("291" === rs1.getString(2))
//        rs1.close()
//
//        val rs2 = statement.executeQuery("SET hive.cli.print.header")
//        rs2.next()
//        assert("hive.cli.print.header" === rs2.getString(1))
//        assert("true" === rs2.getString(2))
//        rs2.close()
//      },
//
//      // third session, we get the latest session status, supposed to be the
//      // default value
//      { statement =>
//
//        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
//        rs1.next()
//        assert(defaultV1 === rs1.getString(1))
//        rs1.close()
//
//        val rs2 = statement.executeQuery("SET hive.cli.print.header")
//        rs2.next()
//        assert(defaultV2 === rs2.getString(1))
//        rs2.close()
//      },
//
//      // try to access the cached data in another session
//      { statement =>
//
//        // Cached temporary table can't be accessed by other sessions
//        intercept[SQLException] {
//          statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
//        }
//
//        val plan = statement.executeQuery("explain select key from test_map ORDER BY key DESC")
//        plan.next()
//        plan.next()
//        assert(plan.getString(1).contains("Scan In-memory table `test_table`"))
//
//        val rs = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
//        val buf = new collection.mutable.ArrayBuffer[Int]()
//        while (rs.next()) {
//          buf += rs.getInt(1)
//        }
//        rs.close()
//        assert(buf === data)
//      },
//
//      // switch another database
//      { statement =>
//        statement.execute("USE db1")
//
//        // there is no test_map table in db1
//        intercept[SQLException] {
//          statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
//        }
//
//        statement.execute("CREATE TABLE test_map2(key INT, value STRING)")
//      },
//
//      // access default database
//      { statement =>
//
//        // current database should still be `default`
//        intercept[SQLException] {
//          statement.executeQuery("SELECT key FROM test_map2")
//        }
//
//        statement.execute("USE db1")
//        // access test_map2
//        statement.executeQuery("SELECT key from test_map2")
//      }
//    )
//  }
//
//  // This test often hangs and then times out, leaving the hanging processes.
//  // Let's ignore it and improve the test.
//  ignore("test jdbc cancel") {
//    withJdbcStatement("test_map") { statement =>
//      val queries = Seq(
//        "CREATE TABLE test_map(key INT, value STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")
//
//      queries.foreach(statement.execute)
//      implicit val ec = ExecutionContext.fromExecutorService(
//        ThreadUtils.newDaemonSingleThreadExecutor("test-jdbc-cancel"))
//      try {
//        // Start a very-long-running query that will take hours to finish, then cancel it in order
//        // to demonstrate that cancellation works.
//        val f = Future {
//          statement.executeQuery(
//            "SELECT COUNT(*) FROM test_map " +
//            List.fill(10)("join test_map").mkString(" "))
//        }
//        // Note that this is slightly race-prone: if the cancel is issued before the statement
//        // begins executing then we'll fail with a timeout. As a result, this fixed delay is set
//        // slightly more conservatively than may be strictly necessary.
//        Thread.sleep(1000)
//        statement.cancel()
//        val e = intercept[SparkException] {
//          ThreadUtils.awaitResult(f, 3.minute)
//        }.getCause
//        assert(e.isInstanceOf[SQLException])
//        assert(e.getMessage.contains("cancelled"))
//
//        // Cancellation is a no-op if spark.sql.hive.thriftServer.async=false
//        statement.executeQuery("SET spark.sql.hive.thriftServer.async=false")
//        try {
//          val sf = Future {
//            statement.executeQuery(
//              "SELECT COUNT(*) FROM test_map " +
//                List.fill(4)("join test_map").mkString(" ")
//            )
//          }
//          // Similarly, this is also slightly race-prone on fast machines where the query above
//          // might race and complete before we issue the cancel.
//          Thread.sleep(1000)
//          statement.cancel()
//          val rs1 = ThreadUtils.awaitResult(sf, 3.minute)
//          rs1.next()
//          assert(rs1.getInt(1) === math.pow(5, 5))
//          rs1.close()
//
//          val rs2 = statement.executeQuery("SELECT COUNT(*) FROM test_map")
//          rs2.next()
//          assert(rs2.getInt(1) === 5)
//          rs2.close()
//        } finally {
//          statement.executeQuery("SET spark.sql.hive.thriftServer.async=true")
//        }
//      } finally {
//        ec.shutdownNow()
//      }
//    }
//  }
//
//  test("test add jar") {
//    withMultipleConnectionJdbcStatement("smallKV", "addJar")(
//      {
//        statement =>
//          val jarFile = HiveTestUtils.getHiveHcatalogCoreJar.getCanonicalPath
//
//          statement.executeQuery(s"ADD JAR $jarFile")
//      },
//
//      {
//        statement =>
//          val queries = Seq(
//            "CREATE TABLE smallKV(key INT, val STRING)",
//            s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE smallKV",
//            """CREATE TABLE addJar(key string)
//              |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
//            """.stripMargin)
//
//          queries.foreach(statement.execute)
//
//          statement.executeQuery(
//            """
//              |INSERT INTO TABLE addJar SELECT 'k1' as key FROM smallKV limit 1
//            """.stripMargin)
//
//          val actualResult =
//            statement.executeQuery("SELECT key FROM addJar")
//          val actualResultBuffer = new collection.mutable.ArrayBuffer[String]()
//          while (actualResult.next()) {
//            actualResultBuffer += actualResult.getString(1)
//          }
//          actualResult.close()
//
//          val expectedResult =
//            statement.executeQuery("SELECT 'k1'")
//          val expectedResultBuffer = new collection.mutable.ArrayBuffer[String]()
//          while (expectedResult.next()) {
//            expectedResultBuffer += expectedResult.getString(1)
//          }
//          expectedResult.close()
//
//          assert(expectedResultBuffer === actualResultBuffer)
//      }
//    )
//  }
//
//  test("Checks Hive version via SET -v") {
//    withJdbcStatement() { statement =>
//      val resultSet = statement.executeQuery("SET -v")
//
//      val conf = mutable.Map.empty[String, String]
//      while (resultSet.next()) {
//        conf += resultSet.getString(1) -> resultSet.getString(2)
//      }
//
//      if (HiveUtils.isHive23) {
//        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("2.3.5"))
//      } else {
//        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("1.2.1"))
//      }
//    }
//  }
//
//  test("Checks Hive version via SET") {
//    withJdbcStatement() { statement =>
//      val resultSet = statement.executeQuery("SET")
//
//      val conf = mutable.Map.empty[String, String]
//      while (resultSet.next()) {
//        conf += resultSet.getString(1) -> resultSet.getString(2)
//      }
//
//      if (HiveUtils.isHive23) {
//        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("2.3.5"))
//      } else {
//        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("1.2.1"))
//      }
//    }
//  }
//
//  test("SPARK-11595 ADD JAR with input path having URL scheme") {
//    withJdbcStatement("test_udtf") { statement =>
//      try {
//        val jarPath = "../hive/src/test/resources/TestUDTF.jar"
//        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"
//
//        Seq(
//          s"ADD JAR $jarURL",
//          s"""CREATE TEMPORARY FUNCTION udtf_count2
//             |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
//           """.stripMargin
//        ).foreach(statement.execute)
//
//        val rs1 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")
//
//        assert(rs1.next())
//        assert(rs1.getString(1) === "Function: udtf_count2")
//
//        assert(rs1.next())
//        assertResult("Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2") {
//          rs1.getString(1)
//        }
//
//        assert(rs1.next())
//        assert(rs1.getString(1) === "Usage: N/A.")
//
//        val dataPath = "../hive/src/test/resources/data/files/kv1.txt"
//
//        Seq(
//          "CREATE TABLE test_udtf(key INT, value STRING)",
//          s"LOAD DATA LOCAL INPATH '$dataPath' OVERWRITE INTO TABLE test_udtf"
//        ).foreach(statement.execute)
//
//        val rs2 = statement.executeQuery(
//          "SELECT key, cc FROM test_udtf LATERAL VIEW udtf_count2(value) dd AS cc")
//
//        assert(rs2.next())
//        assert(rs2.getInt(1) === 97)
//        assert(rs2.getInt(2) === 500)
//
//        assert(rs2.next())
//        assert(rs2.getInt(1) === 97)
//        assert(rs2.getInt(2) === 500)
//      } finally {
//        statement.executeQuery("DROP TEMPORARY FUNCTION udtf_count2")
//      }
//    }
//  }
//
//  test("SPARK-11043 check operation log root directory") {
//    val expectedLine =
//      "Operation log root directory is created: " + operationLogPath.getAbsoluteFile
//    val bufferSrc = Source.fromFile(logPath)
//    Utils.tryWithSafeFinally {
//      assert(bufferSrc.getLines().exists(_.contains(expectedLine)))
//    } {
//      bufferSrc.close()
//    }
//  }
//
//  test("SPARK-23547 Cleanup the .pipeout file when the Hive Session closed") {
//    def pipeoutFileList(sessionID: UUID): Array[File] = {
//      lScratchDir.listFiles(new FilenameFilter {
//        override def accept(dir: File, name: String): Boolean = {
//          name.startsWith(sessionID.toString) && name.endsWith(".pipeout")
//        }
//      })
//    }
//
//    withCLIServiceClient { client =>
//      val user = System.getProperty("user.name")
//      val sessionHandle = client.openSession(user, "")
//      val sessionID = sessionHandle.getSessionId
//
//      if (HiveUtils.isHive23) {
//        assert(pipeoutFileList(sessionID).length == 2)
//      } else {
//        assert(pipeoutFileList(sessionID).length == 1)
//      }
//
//      client.closeSession(sessionHandle)
//
//      assert(pipeoutFileList(sessionID).length == 0)
//    }
//  }
//
//  test("SPARK-24829 Checks cast as float") {
//    withJdbcStatement() { statement =>
//      val resultSet = statement.executeQuery("SELECT CAST('4.56' AS FLOAT)")
//      resultSet.next()
//      assert(resultSet.getString(1) === "4.56")
//    }
//  }
//}

//class HiveThriftBinaryServerForProxySuite extends HiveThriftJdbcForProxyTest {
//  override def mode: ServerMode.Value = ServerMode.binary
//
//  def showResult(result: ResultSet): Unit = {
//    while (result.next()) {
//    }
//  }
//
//  test("SPARK-28419 Check Proxy user") {
//    withJdbcStatement("user1") { statement =>
//      val queries = Seq(
//        "SET spark.sql.shuffle.partitions=3",
//        "CREATE TABLE test_user1(key INT, val STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_user1")
//
//      queries.foreach(query => {
//        statement.execute(query)
//      })
//
//      assertResult(5, "Row count mismatch") {
//        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_user1")
//        resultSet.next()
//        resultSet.getInt(1)
//      }
//    }
//
//    withJdbcStatement("user2") { statement =>
//      val queries = Seq(
//        "SET spark.sql.shuffle.partitions=3",
//        "CREATE TABLE test_user2(key INT, val STRING)",
//        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_user2")
//
//      queries.foreach(query => {
//        statement.execute(query)
//      })
//
//      //      assertResult(5, "Row count mismatch") {
//      val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_user1")
//      //        resultSet.next()
//      //        resultSet.getInt(1)
//      //      }
//
//      assertResult(5, "Row count mismatch") {
//        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_user2")
//        resultSet.next()
//        resultSet.getInt(1)
//      }
//    }
//  }
//
//  test("SPARK-28419 CHECK PROXY USER") {
//    withJdbcStatement("user1") { statement =>
//      val resultSet = statement.executeQuery("SELECT CAST('4.56' AS FLOAT)")
//      resultSet.next()
//      assert(resultSet.getString(1) === "4.56")
//    }
//  }
//}


class HiveThriftBinaryServerForProxyWithSecureSuite extends HiveThriftJdbcForProxyTest(true) {
  override def mode: ServerMode.Value = ServerMode.binary

  test("SPARK-28419 Check Proxy user") {
    withJdbcStatement("user1") { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "CREATE TABLE test_user1(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_user1")

      queries.foreach(query => {
        statement.execute(query)
      })

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_user1")
        resultSet.next()
        resultSet.getInt(1)
      }
    }

    withJdbcStatement("user2") { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "CREATE TABLE test_user2(key INT, val STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_user2")

      queries.foreach(query => {
        statement.execute(query)
      })

      //      assertResult(5, "Row count mismatch") {
      val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_user1")
      //        resultSet.next()
      //        resultSet.getInt(1)
      //      }

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_user2")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }
}

abstract class HiveThriftJdbcForProxyTest(secure: Boolean = false) extends HiveThriftServer2ForProxyTest(secure) {
  Utils.classForName(classOf[HiveDriver].getCanonicalName)

  //  hive.server2.proxy.user=${user};
  private def jdbcUriForProxu(user: String): String =
    if (secure) {
      if (mode == ServerMode.http) {
        s"""jdbc:hive2://localhost:$serverPort/
           |default;
           |principal=${clientPrincipal(HIVE_HIVESERVER2_USER)};
           |hive.server2.proxy.user=${user};
           |hive.server2.transport.mode=http;
           |hive.server2.thrift.http.path=cliservice;
           |${hiveConfList}#${hiveVarList}
     """.stripMargin.split("\n").mkString.trim
      } else {
        s"""jdbc:hive2://localhost:$serverPort/default;
           |principal=${clientPrincipal(HIVE_HIVESERVER2_USER)};
           |hive.server2.proxy.user=${user};
           """
          .stripMargin.split("\n").mkString.trim
      }
    } else {
      if (mode == ServerMode.http) {
        s"""jdbc:hive2://localhost:$serverPort/
           |default;
           |hive.server2.proxy.user=${user};
           |hive.server2.transport.mode=http;
           |hive.server2.thrift.http.path=cliservice;
           |${hiveConfList}#${hiveVarList}
     """.stripMargin.split("\n").mkString.trim
      } else {
        s"jdbc:hive2://localhost:$serverPort/default;hive.server2.proxy.user=${user};${hiveConfList}#${hiveVarList}"
      }
    }

  def withMultipleConnectionJdbcStatement(proxyUser: String, tableNames: String*)(fs: (Statement => Unit)*) = {
    println(proxyUser)
    println(jdbcUriForProxu(proxyUser))
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    if (secure) {
      kinit(clientPrincipal(STS_USER), KEYTAB_BASE_DIR + "/spark.keytab")
    }
    val connections = fs.map { _ => new HiveConnection(jdbcUriForProxu(proxyUser), new Properties()) }
    val statements = connections.map(_.createStatement())
    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      tableNames.foreach { name =>
        // TODO: Need a better way to drop the view.
        if (name.toUpperCase(Locale.ROOT).startsWith("VIEW")) {
          statements(0).execute(s"DROP VIEW IF EXISTS $name")
        } else {
          statements(0).execute(s"DROP TABLE IF EXISTS $name")
        }
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }


  def withDatabase(dbNames: String*)(fs: (Statement => Unit)*) {
    if (secure) {
      kinit(clientPrincipal(STS_USER), KEYTAB_BASE_DIR + "/spark.keytab")
    }
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUriForProxu(user), user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      dbNames.foreach { name =>
        statements(0).execute(s"DROP DATABASE IF EXISTS $name")
      }
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withJdbcStatement(proxyUser: String, tableNames: String*)(f: Statement => Unit) {
    println("withJdbcStatement => " + proxyUser)
    withMultipleConnectionJdbcStatement(proxyUser, tableNames: _*)(f)
  }
}

abstract class HiveThriftServer2ForProxyTest(secure: Boolean) extends SparkFunSuite with BeforeAndAfterAll with Logging {
  def mode: ServerMode.Value

  import HiveThriftServer2ForProxyTest._

  private val CLASS_NAME = HiveThriftServer2.getClass.getCanonicalName.stripSuffix("$")
  private val LOG_FILE_MARK = s"starting $CLASS_NAME, logging to "

  protected val startScript = "../../sbin/start-thriftserver.sh".split("/").mkString(File.separator)
  protected val stopScript = "../../sbin/stop-thriftserver.sh".split("/").mkString(File.separator)

  private var listeningPort: Int = _

  protected def serverPort: Int = listeningPort

  protected val hiveConfList = "a=avalue;b=bvalue"
  protected val hiveVarList = "c=cvalue;d=dvalue"

  protected def user = System.getProperty("user.name")

  protected var metastoreUri: String = _

  protected def metastoreJdbcUri = s"jdbc:derby:;databaseName=$metastorePath;create=true"

  protected var warehousePath: File = _
  protected var metastorePath: File = _

  private var driverClassPath: String = _
  private val pidDir: File = Utils.createTempDir(namePrefix = "thriftserver-pid")
  protected var logPath: File = _
  protected var operationLogPath: File = _
  protected var lScratchDir: File = _
  private var logTailingProcess: Process = _
  private var diagnosisBuffer: ArrayBuffer[String] = ArrayBuffer.empty[String]

  private var kdcReady: Boolean = false
  private var miniKDC: MiniKdc = _
  private var miniDFSCluster: MiniDFSCluster = _
  private var miniFS: FileSystem = _
  private var hadoopConfPath: File = _
  private var nameDir: File = _
  private var dataDir: File = _
  private var miniClusterPrepare: Boolean = false

  protected var KEYTAB_BASE_DIR: File = _
  protected val STS_USER = "hadoop/sparkthriftserver"
  protected val HIVE_METASTORE_USER = "hive/metastore"
  protected val DFS_NN_USER = s"nn/${getHostNameForLiunx()}"
  protected val DFS_DN_USER = s"dn/${getHostNameForLiunx()}"
  protected val DFS_HTTP_USER = s"HTTP/${getHostNameForLiunx()}"
  protected val HIVE_HIVESERVER2_USER = "hive/hiveserver"


  protected def extraConf: Seq[String] = Nil

  protected def extraConfPair: Map[String, String] = Map.empty[String, String]

  protected def serverStartCommand(port: Int) = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    driverClassPath = {
      // Writes a temporary log4j.properties and prepend it to driver classpath, so that it
      // overrides all other potential log4j configurations contained in other dependency jar files.
      val tempLog4jConf = Utils.createTempDir().getCanonicalPath

      Files.write(
        """log4j.rootCategory=INFO, console
          |log4j.appender.console=org.apache.log4j.ConsoleAppender
          |log4j.appender.console.target=System.err
          |log4j.appender.console.layout=org.apache.log4j.PatternLayout
          |log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
        """.stripMargin,
        new File(s"$tempLog4jConf/log4j.properties"),
        StandardCharsets.UTF_8)

      tempLog4jConf
    }

    if (secure) {
      s"""$startScript
         |  --master local
         |  --deploy-mode client
         |  --conf spark.kerberos.keytab=${KEYTAB_BASE_DIR}/spark.keytab
         |  --conf spark.kerberos.principal=${clientPrincipal(STS_USER)}
         |  --hiveconf ${ConfVars.HIVE_SERVER2_AUTHENTICATION}=KERBEROS
         |  --hiveconf ${ConfVars.HIVE_SERVER2_ENABLE_DOAS}=true
         |  --hiveconf ${ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB}=${KEYTAB_BASE_DIR}/hiveserver2.keytab
         |  --hiveconf ${ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL}=${clientPrincipal(HIVE_HIVESERVER2_USER)}
         |  --hiveconf ${ConfVars.METASTORE_KERBEROS_KEYTAB_FILE}=${KEYTAB_BASE_DIR}/hivemetastore.keytab
         |  --hiveconf ${ConfVars.METASTORE_KERBEROS_PRINCIPAL}=${clientPrincipal(HIVE_METASTORE_USER)}
         |  --hiveconf ${ConfVars.METASTOREURIS}=$metastoreUri
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
         |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
         |  --hiveconf ${ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION}=$operationLogPath
         |  --hiveconf ${ConfVars.LOCALSCRATCHDIR}=$lScratchDir
         |  --hiveconf $portConf=$port
         |  --driver-class-path $driverClassPath
         |  --driver-java-options -Djava.security.krb5.conf=${miniKDC.getKrb5conf.getAbsolutePath}
         |  --conf spark.ui.enabled=false
         |  ${extraConf.mkString("\n")}
     """.stripMargin.split("\\s+").toSeq
    } else {
      s"""$startScript
         |  --master local
         |  --hiveconf ${ConfVars.METASTOREURIS}=$metastoreUri
         |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
         |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
         |  --hiveconf ${ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION}=$operationLogPath
         |  --hiveconf ${ConfVars.LOCALSCRATCHDIR}=$lScratchDir
         |  --hiveconf $portConf=$port
         |  --driver-class-path $driverClassPath
         |  --driver-java-options -Dlog4j.debug -Djava.security.krb5.conf=${miniKDC.getKrb5conf.getAbsolutePath}
         |  --conf spark.ui.enabled=false
         |  ${extraConf.mkString("\n")}
     """.stripMargin.split("\\s+").toSeq
    }
  }

  /**
    * String to scan for when looking for the thrift binary endpoint running.
    * This can change across Hive versions.
    */
  val THRIFT_BINARY_SERVICE_LIVE = "Starting ThriftBinaryCLIService on port"

  /**
    * String to scan for when looking for the thrift HTTP endpoint running.
    * This can change across Hive versions.
    */
  val THRIFT_HTTP_SERVICE_LIVE = "Started ThriftHttpCLIService in http"

  val SERVER_STARTUP_TIMEOUT = 3.minutes

  def clientPrincipal(name: String): String = {
    assert(kdcReady, "KDC should be set up beforehand")
    name + "@" + miniKDC.getRealm()
  }

  def kinit(principal: String, keyTab: String): Unit = {
    println(s"Kinit with principal [ ${principal} ], keytab [ ${keyTab} ]")
    val krb5 = miniKDC.getKrb5conf.getAbsolutePath
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.set("hadoop.security.authentication", "Kerberos")
    System.setProperty("java.security.krb5.conf", krb5)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab(principal, keyTab)
  }

  private class RunMS(var port: String) extends Runnable {

    override def run(): Unit = {
      try {
        if (secure) {
          HiveMetaStore.main(Array[String](
            s"-p",
            s"${port}",
            s"--hiveconf",
            s"${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri",
            "--hiveconf",
            s"${ConfVars.METASTORE_CONNECTION_DRIVER}=org.apache.derby.jdbc.EmbeddedDriver",
            "--hiveconf",
            s"hive.metastore.local=true",
            s"--hiveconf",
            s"${ConfVars.METASTOREWAREHOUSE}=$warehousePath",
            "--hiveconf",
            s"hive.metastore.kerberos.keytab.file=${KEYTAB_BASE_DIR}/hivemetastore.keytab",
            "--hiveconf",
            s"hive.metastore.kerberos.principal=${clientPrincipal(HIVE_METASTORE_USER)}"))
        } else {
          HiveMetaStore.main(Array[String](
            s"-p",
            s"${port}",
            s"--hiveconf",
            s"${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri",
            "--hiveconf",
            s"${ConfVars.METASTORE_CONNECTION_DRIVER}=org.apache.derby.jdbc.EmbeddedDriver",
            "--hiveconf",
            s"hive.metastore.local=true",
            s"--hiveconf",
            s"${ConfVars.METASTOREWAREHOUSE}=$warehousePath"))
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace(System.err)
          assert(false)
      }
    }
  }

  private def startRemoteMS(msPort: Int) = {
    val t = new Thread(new RunMS(msPort.toString))
    println(s"Starting MS on port:${msPort}")
    t.start()

    // Wait a little bit for the metastore to start.
    Thread.sleep(5000)
  }


  private def setUpMiniKdc(): Unit = {
    println("Starting MiniKDC")
    try {
      val kdcDir = Utils.createTempDir()
      val kdcConf = MiniKdc.createConf
      kdcConf.setProperty("kdc.bind.address", getHostNameForLiunx())
      kdcConf.setProperty("kdc.port", s"${serverPort + 20000}")
      miniKDC = new MiniKdc(kdcConf, kdcDir)
      miniKDC.start()
      println(s"kdcport ${miniKDC.getPort}")
      println(s"kdchost ${miniKDC.getHost}")
      kdcReady = true
    } catch {
      case e: Exception => println(e.printStackTrace())
    }
  }

  private def core_site =
    s"""
       |<?xml version="1.0"?>
       |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
       |
      |<!-- Put site-specific property overrides in this file. -->
       |
      |<configuration>
       |  <property>
       |    <name>hadoop.security.authorization</name>
       |    <value>true</value>
       |  </property>
       |  <property>
       |    <name>hadoop.security.authentication</name>
       |    <value>kerberos</value>
       |  </property>
       |  <property>
       |    <name>hadoop.proxyuser.hadoop.hosts</name>
       |    <value>*</value>
       |  </property>
       |  <property>
       |    <name>hadoop.proxyuser.hadoop.users</name>
       |    <value>*</value>
       |  </property>
       |  <property>
       |    <name>hadoop.proxyuser.hadoop.groups</name>
       |    <value>*</value>
       |  </property>
       |  <property>
       |    <name>hadoop.proxyuser.httpfs.hosts</name>
       |    <value>*</value>
       |  </property>
       |  <property>
       |    <name>hadoop.proxyuser.httpfs.users</name>
       |    <value>*</value>
       |  </property>
       |  <property>
       |    <name>hadoop.proxyuser.httpfs.groups</name>
       |    <value>*</value>
       |  </property>
       |  <property>
       |   <name>hadoop.security.auth_to_local</name>
       |   <value>
       |       RULE:[2:${"1@$0"}](nn/.*@.*ESRICHINA.COM)s/.*/hdfs/
       |       RULE:[2:${"1@$0"}](sn/.*@.*ESRICHINA.COM)s/.*/hdfs/
       |       RULE:[2:${"1@$0"}](dn/.*@.*ESRICHINA.COM)s/.*/hdfs/
       |       RULE:[2:${"1@$0"}](nm/.*@.*ESRICHINA.COM)s/.*/yarn/
       |       RULE:[2:${"1@$0"}](rm/.*@.*ESRICHINA.COM)s/.*/yarn/
       |       RULE:[2:${"1@$0"}](tl/.*@.*ESRICHINA.COM)s/.*/yarn/
       |       RULE:[2:${"1@$0"}](jhs/.*@.*ESRICHINA.COM)s/.*/mapred/
       |       RULE:[2:${"1@$0"}](HTTP/.*@.*ESRICHINA.COM)s/.*/hdfs/
       |       DEFAULT
       |   </value>
       |  </property>
       |</configuration>
    """.stripMargin.trim


  private def hdfs_site =
    s"""
       |<?xml version="1.0"?>
       |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
       |<configuration>
       |<property>
       |  <name>dfs.block.access.token.enable</name>
       |  <value>true</value>
       |</property>
       |<property>
       |  <name>dfs.namenode.name.dir</name>
       |  <value>${nameDir.getAbsolutePath}</value>
       |</property>
       |<property>
       |  <name>dfs.namenode.hosts</name>
       |  <value>${getHostNameForLiunx()}</value>
       |</property>
       |<property>
       |  <name>dfs.datanode.data.dir</name>
       |  <value>${dataDir.getAbsolutePath}</value>
       |</property>
       |<!-- NameNode security config -->
       |<property>
       |  <name>dfs.namenode.kerberos.principal</name>
       |  <value>${clientPrincipal(DFS_NN_USER)}</value>
       |</property>
       |<property>
       |  <name>dfs.namenode.keytab.file</name>
       |  <value>${KEYTAB_BASE_DIR}/minicluster.keytab</value>
       |</property>
       |<property>
       |  <name>dfs.namenode.kerberos.internal.spnego.principal</name>
       |  <value>${clientPrincipal(DFS_HTTP_USER)}</value>
       |</property>
       |
       |<!--Secondary NameNode security config -->
       |<property>
       |  <name>dfs.secondary.namenode.kerberos.principal</name>
       |  <value>${clientPrincipal(DFS_NN_USER)}</value>
       |</property>
       |<property>
       |  <name>dfs.secondary.namenode.keytab.file</name>
       |  <value>${KEYTAB_BASE_DIR}/minicluster.keytab</value>
       |</property>
       |<property>
       |  <name>dfs.secondary.namenode.kerberos.internal.spnego.principal</name>
       |  <value>${clientPrincipal(DFS_HTTP_USER)}</value>
       |</property>
       |
       |<property>
       |  <name>dfs.datanode.kerberos.principal</name>
       |  <value>${clientPrincipal(DFS_DN_USER)}</value>
       |</property>
       |<property>
       |  <name>dfs.datanode.keytab.file</name>
       |  <value>${KEYTAB_BASE_DIR}/minicluster.keytab</value>
       |</property>
       |<property>
       |  <name>dfs.datanode.data.dir.perm</name>
       |  <value>700</value>
       |</property>
       |<property>
       |  <name>dfs.datanode.hostname</name>
       |  <value>${getHostNameForLiunx()}</value>
       |</property>
       |<property>
       |  <name>dfs.datanode.address</name>
       |  <value>0.0.0.0:50004</value>
       |</property>
       |<property>
       |  <name>dfs.datanode.http.address</name>
       |  <value>0.0.0.0:50006</value>
       |</property>
       |<property>
       |  <name>ignore.secure.ports.for.testing</name>
       |  <value>true</value>
       |</property>
       |
       |<!--configure secure WebHDFS -->
       |<property>
       |  <name>dfs.webhdfs.enabled</name>
       |  <value>true</value>
       |</property>
       |<property>
       |  <name>dfs.web.authentication.kerberos.principal</name>
       |  <value>${clientPrincipal(DFS_HTTP_USER)}</value>
       |</property>
       |<property>
       |  <name>dfs.web.authentication.kerberos.keytab</name>
       |  <value>${KEYTAB_BASE_DIR}/minicluster.keytab</value>
       |</property>
       |<property>
       |  <name>dfs.permissions.supergroup</name>
       |  <value>hdfs</value>
       |  <description>The name of the group of super-users.</description>
       |</property>
       |</configuration>
    """.stripMargin.trim

  private def yarn_site =
    """
      |<?xml version="1.0"?>
      |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      |
      |<!-- Put site-specific property overrides in this file. -->
      |
      |<configuration>
      |</configuration>
    """.stripMargin.trim

  private def mapred_site =
    """
      |<?xml version="1.0"?>
      |<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
      |
      |<!-- Put site-specific property overrides in this file. -->
      |
      |<configuration>
      |</configuration>
    """.stripMargin.trim


  def createHDPConf(hdpConfPath: String): Unit = {
    Files.write(core_site,
      new File(hdpConfPath + "/core-site.xml"),
      StandardCharsets.UTF_8)

    Files.write(hdfs_site,
      new File(hdpConfPath + "/hdfs-site.xml"),
      StandardCharsets.UTF_8)

    Files.write(yarn_site,
      new File(hdpConfPath + "/yarn-site.xml"),
      StandardCharsets.UTF_8)

    Files.write(mapred_site,
      new File(hdpConfPath + "/mapred-site.xml"),
      StandardCharsets.UTF_8)
  }

  private class RunMiniCluster extends Runnable {
    override def run(): Unit = {
      try {
        val dataNodes = 0
        // There will be 4 data nodes
        //    val taskTrackers = 1
        // There will be 4 task tracker nodes


        val config = new Configuration
        hadoopConfPath.listFiles().foreach(file => {
          config.addResource(new FileInputStream(file))
        })

        //        if (secure) {
        //          config.set("dfs.block.access.token.enable", "true")
        //          config.set("hadoop.security.authentication", "kerberos")
        //          config.set("hadoop.security.authorization", "true")
        //          config.set("hadoop.rpc.protection", "authentication")
        //          config.set("dfs.namenode.kerberos.principal", clientPrincipal(DFS_NN_USER))
        //          config.set("dfs.namenode.kerberos.https.principal", clientPrincipal(DFS_HTTP_USER))
        //          config.set("dfs.namenode.keytab.file", s"${KEYTAB_BASE_DIR}/minicluster.keytab")
        //          config.set("dfs.secondary.namenode.kerberos.principal", clientPrincipal(DFS_NN_USER))
        //          config.set("dfs.secondary.namenode.keytab.file", s"${KEYTAB_BASE_DIR}/minicluster.keytab")
        //          config.set("dfs.namenode.kerberos.internal.spnego.principal", clientPrincipal(DFS_HTTP_USER))
        //          config.set("dfs.permissions.supergroup", "supergroup")
        //          config.set("dfs.namenode.https-address", "0.0.0.0:50470")
        //          config.set("dfs.data.transfer.protection", "authentication")
        //          // datanode
        //          config.set("dfs.datanode.data.dir.perm", "700")
        //          config.set("dfs.datanode.address", "0.0.0.0:1004")
        //          config.set("dfs.datanode.http.address", "0.0.0.0:1006")
        //          config.set("dfs.datanode.https.address", "0.0.0.0:50470")
        //          config.set("dfs.datanode.kerberos.principal", clientPrincipal(DFS_DN_USER))
        //          config.set("dfs.datanode.keytab.file", s"${KEYTAB_BASE_DIR}/minicluster.keytab")
        //          config.set("dfs.encrypt.data.transfer", "false")
        //          // web
        //          config.set("dfs.webhdfs.enabled", "true")
        //          config.set("dfs.https.enable", "true")
        //          config.set("dfs.http.policy", "HTTP_AND_HTTPS")
        //          config.set("dfs.web.authentication.kerberos.principal", clientPrincipal(DFS_HTTP_USER))
        //          config.set("dfs.web.authentication.kerberos.keytab", s"${KEYTAB_BASE_DIR}/minicluster.keytab")
        //
        //          config.set("hadoop.proxyuser.hadoop.hosts", "*")
        //          config.set("hadoop.proxyuser.hadoop.users", "*")
        //          config.set("hadoop.proxyuser.hive.hosts", "*")
        //          config.set("hadoop.proxyuser.hive.users", "*")
        //        }

        // Builds and starts the mini dfs and mapreduce clusters
        System.setProperty("hadoop.log.dir", s"target/tmp/logs/")
        miniDFSCluster = new MiniDFSCluster.Builder(config).build()
        miniFS = miniDFSCluster.getFileSystem
        miniClusterPrepare = true
      } catch {
        case e: Exception => {
          println(e.printStackTrace())
          throw e
        }

      }
    }
  }

  private def startMiniCluster() = {
    val t = new Thread(new RunMiniCluster)
    println(s"Starting MiniCluster..........")
    t.start()

    var count = 0
    // Wait a little bit for the metastore to start.
    while (!miniClusterPrepare & count < 20) {
      count += 1
      println(s"miniClusterPrepare ${miniClusterPrepare}")
      println("Sleep next 10s")
      Thread.sleep(10000)
    }
  }


  /**
    * Init Kerberos Environment needed keytabs and principals
    */
  private def initKerberosKeyTabAndPrincipals(): Unit = {
    try {
      KEYTAB_BASE_DIR.setExecutable(true, false)
      KEYTAB_BASE_DIR.setReadable(true, false)
      prepareHiveMSSecure()
      prepareMiniClusterSecure()
      prepareSparkSecure()
      prepareThriftServerSecure()
      KEYTAB_BASE_DIR.listFiles().foreach(file => {
        file.setExecutable(true, false)
        file.setReadable(true, false)
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    }

  }

  private def prepareHiveMSSecure(): Unit = {
    val zkServerKeytabFile = new File(KEYTAB_BASE_DIR, "hivemetastore.keytab")
    miniKDC.createPrincipal(zkServerKeytabFile, HIVE_METASTORE_USER)
  }

  private def prepareMiniClusterSecure(): Unit = {
    val miniClusterNNKeyTabFile = new File(KEYTAB_BASE_DIR, "minicluster.keytab")
    miniKDC.createPrincipal(miniClusterNNKeyTabFile, DFS_NN_USER, DFS_HTTP_USER, DFS_DN_USER)
  }

  private def prepareThriftServerSecure(): Unit = {
    val thriftServerKeyTabFile = new File(KEYTAB_BASE_DIR, "hiveserver2.keytab")
    miniKDC.createPrincipal(thriftServerKeyTabFile, HIVE_HIVESERVER2_USER)
  }

  private def prepareSparkSecure(): Unit = {
    val sparkSecureKeyTab = new File(KEYTAB_BASE_DIR, "spark.keytab")
    miniKDC.createPrincipal(sparkSecureKeyTab, STS_USER)
  }

  private def startThriftServer(port: Int, msPort: Int, attempt: Int) = {

    metastoreUri = s"thrift://localhost:${msPort}"
    warehousePath = Utils.createTempDir()
    warehousePath.delete()
    metastorePath = Utils.createTempDir()
    metastorePath.delete()
    operationLogPath = Utils.createTempDir()
    operationLogPath.delete()
    lScratchDir = Utils.createTempDir()
    lScratchDir.delete()
    KEYTAB_BASE_DIR = Utils.createTempDir()
    println(KEYTAB_BASE_DIR)
    hadoopConfPath = Utils.createTempDir()
    println(s"hadoopConfDir => ${hadoopConfPath.getAbsolutePath}")
    nameDir = Utils.createTempDir()
    dataDir = Utils.createTempDir()
    logPath = null
    logTailingProcess = null

    if (secure) {
      // start miniKDC
      setUpMiniKdc()
      // init keytabs and principals
      initKerberosKeyTabAndPrincipals()

      kinit(clientPrincipal(STS_USER), KEYTAB_BASE_DIR + "/spark.keytab")
      // create kerberos hadoop conf
      createHDPConf(hadoopConfPath.getAbsolutePath)
      // start miniCluster
      startMiniCluster()

      println(s"Login user ${UserGroupInformation.getLoginUser.getUserName}")
    }

    val command = serverStartCommand(port)

    println(command)

    diagnosisBuffer ++=
      s"""
         |### Attempt $attempt ###
         |HiveThriftServer2 command line: $command
         |Listening port: $port
         |System user: $user
       """.stripMargin.split("\n")


    println(s"Tryting to start Remote Hive MetaStore: port: ${msPort}")
    logInfo(s"Tryting to start Remote Hive MetaStore: port: ${msPort}")
    startRemoteMS(msPort)

    println(s"HiveVersion: ${HiveVersionInfo.getVersion}")
    logInfo(s"Trying to start HiveThriftServer2: port=$port, mode=$mode, attempt=$attempt")

    logPath = {
      val lines = if (secure) {
        println("start Thrift In secure mode")
        Utils.executeAndGetOutput(
          command = command,
          extraEnvironment = Map(
            // HADOOP_CONF_DIR
            "HADOOP_CONF_DIR" -> hadoopConfPath.getCanonicalPath,
            // Disables SPARK_TESTING to exclude log4j.properties in test directories.
            //            "SPARK_TESTING" -> "0",
            // But set SPARK_SQL_TESTING to make spark-class happy.
            "SPARK_SQL_TESTING" -> "1",
            // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be
            // started at a time, which is not Jenkins friendly.
            "SPARK_PID_DIR" -> pidDir.getCanonicalPath),
          redirectStderr = true)
      } else {
        Utils.executeAndGetOutput(
          command = command,
          extraEnvironment = Map(
            // Disables SPARK_TESTING to exclude log4j.properties in test directories.
            "SPARK_TESTING" -> "0",
            // But set SPARK_SQL_TESTING to make spark-class happy.
            "SPARK_SQL_TESTING" -> "1",
            // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be
            // started at a time, which is not Jenkins friendly.
            "SPARK_PID_DIR" -> pidDir.getCanonicalPath),
          redirectStderr = true)
      }

      logInfo(s"COMMAND: $command")
      logInfo(s"OUTPUT: $lines")
      lines.split("\n").collectFirst {
        case line if line.contains(LOG_FILE_MARK) => new File(line.drop(LOG_FILE_MARK.length))
      }.getOrElse {
        throw new RuntimeException("Failed to find HiveThriftServer2 log file.")
      }
    }

    val serverStarted = Promise[Unit]()

    // Ensures that the following "tail" command won't fail.
    logPath.createNewFile()
    val successLines = Seq(THRIFT_BINARY_SERVICE_LIVE, THRIFT_HTTP_SERVICE_LIVE)

    logTailingProcess = {
      val command = s"/usr/bin/env tail -n +0 -f ${logPath.getCanonicalPath}".split(" ")
      // Using "-n +0" to make sure all lines in the log file are checked.
      val builder = new ProcessBuilder(command: _*)
      val captureOutput = (line: String) => diagnosisBuffer.synchronized {
        diagnosisBuffer += line

        successLines.foreach { r =>
          if (line.contains(r)) {
            serverStarted.trySuccess(())
          }
        }
      }

      val process = builder.start()

      new ProcessOutputCapturer(process.getInputStream, captureOutput).start()
      new ProcessOutputCapturer(process.getErrorStream, captureOutput).start()
      process
    }

    ThreadUtils.awaitResult(serverStarted.future, SERVER_STARTUP_TIMEOUT)
  }

  private def stopThriftServer(): Unit = {
    // The `spark-daemon.sh' script uses kill, which is not synchronous, have to wait for a while.
    Utils.executeAndGetOutput(
      command = Seq(stopScript),
      extraEnvironment = Map("SPARK_PID_DIR" -> pidDir.getCanonicalPath))
    Thread.sleep(3.seconds.toMillis)

    warehousePath.delete()
    warehousePath = null

    metastorePath.delete()
    metastorePath = null

    operationLogPath.delete()
    operationLogPath = null

    lScratchDir.delete()
    lScratchDir = null

    KEYTAB_BASE_DIR.delete()
    KEYTAB_BASE_DIR = null

    hadoopConfPath.delete()
    hadoopConfPath = null
    nameDir.delete()
    nameDir = null
    dataDir.delete()
    dataDir = null

    Option(logPath).foreach(_.delete())
    logPath = null

    Option(logTailingProcess).foreach(_.destroy())
    logTailingProcess = null
  }

  private def dumpLogs(): Unit = {
    logError(
      s"""
         |=====================================
         |HiveThriftServer2Suite failure output
         |=====================================
         |${diagnosisBuffer.mkString("\n")}
         |=========================================
         |End HiveThriftServer2Suite failure output
         |=========================================
       """.stripMargin)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)
    diagnosisBuffer.clear()

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 1).foldLeft(Try(startThriftServer(listeningPort, listeningPort + 10000, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        stopThriftServer()
        Try(startThriftServer(listeningPort, listeningPort + 10000, attempt))
      }
    }.recover {
      case cause: Throwable =>
        dumpLogs()
        throw cause
    }.get

    logInfo(s"HiveThriftServer2 started successfully")
  }

  override protected def afterAll(): Unit = {
    try {
      Thread.sleep(10000000)
      stopThriftServer()
      logInfo("HiveThriftServer2 stopped")
    } finally {
      super.afterAll()
    }
  }
}

object HiveThriftServer2ForProxyTest {
  def getHostNameForLiunx(): String = {
    try {
      (InetAddress.getLocalHost()).getCanonicalHostName()
    } catch {
      case uhe: UnknownHostException =>
        val host = uhe.getMessage(); // host = "hostname: hostname"
        if (host != null) {
          val colon = host.indexOf(':');
          if (colon > 0) {
            return host.substring(0, colon);
          }
        }
        "UnknownHost"
    }
  }


  def getHostName() {
    if (System.getenv("COMPUTERNAME") != null) {
      return System.getenv("COMPUTERNAME");
    } else {
      return getHostNameForLiunx();
    }
  }
}