package org.apache.spark.sql.hive.thriftserver.kerberos

import org.apache.spark.tags.DockerTest

@DockerTest
object MiniKerberizedDFSCluster {

  def startMiniDFSCluster(hdpConfPath: String): Unit ={

    val namenode = new MiniDFSClusterOnDocker {
      override val imageName: String = "MiniDFSCluster-NameNode"
      override val env: Map[String, String] = Map(
        "HADOOP_CONF_DIR" -> hdpConfPath
      )
      override val usesIpc: Boolean = false
      override def getStartupProcessName: Option[String] = None
    }
  }
}


abstract class MiniDFSClusterOnDocker {
  /**
    * The docker image to be pulled.
    */
  val imageName: String

  /**
    * Environment variables to set inside of the Docker container while launching it.
    */
  val env: Map[String, String]

  /**
    * Wheather or not to use ipc mode for shared memory when starting docker image
    */
  val usesIpc: Boolean

  /**
    * Optional process to run when container starts
    */
  def getStartupProcessName: Option[String]
}
