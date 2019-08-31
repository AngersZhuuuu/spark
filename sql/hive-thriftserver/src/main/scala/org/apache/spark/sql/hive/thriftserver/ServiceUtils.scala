package org.apache.spark.sql.hive.thriftserver

object ServiceUtils {

  /*
   * Get the index separating the user name from domain name (the user's name up
   * to the first '/' or '@').
   *
   * @param userName full user name.
   * @return index of domain match or -1 if not found
   */
  def indexOfDomainMatch(userName: String): Int = {
    if (userName == null) {
      return -1
    }
    val idx = userName.indexOf('/')
    val idx2 = userName.indexOf('@')
    var endIdx = Math.min(idx, idx2) // Use the earlier match.
    // Unless at least one of '/' or '@' was not found, in
    // which case, user the latter match.
    if (endIdx == -1) {
      endIdx = Math.max(idx, idx2)
    }
    endIdx
  }
}
