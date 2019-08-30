package org.apache.spark.sql.hive.thriftserver.auth

abstract class AuthType(authType: String) {

  def getAuthName: String = authType

  override def toString: String = getAuthName
}

case object NOSASL extends AuthType("NOSASL")

case object NONE extends AuthType("NONE")

case object LDAP extends AuthType("LDAP")

case object KERBEROS extends AuthType("KERBEROS")

case object CUSTOM extends AuthType("CUSTOM")

case object PAM extends AuthType("PAM")