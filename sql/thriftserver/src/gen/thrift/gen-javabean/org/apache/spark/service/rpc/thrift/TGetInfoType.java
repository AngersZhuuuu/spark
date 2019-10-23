/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.spark.service.rpc.thrift;


@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)")
public enum TGetInfoType implements org.apache.thrift.TEnum {
  CLI_MAX_DRIVER_CONNECTIONS(0),
  CLI_MAX_CONCURRENT_ACTIVITIES(1),
  CLI_DATA_SOURCE_NAME(2),
  CLI_FETCH_DIRECTION(8),
  CLI_SERVER_NAME(13),
  CLI_SEARCH_PATTERN_ESCAPE(14),
  CLI_DBMS_NAME(17),
  CLI_DBMS_VER(18),
  CLI_ACCESSIBLE_TABLES(19),
  CLI_ACCESSIBLE_PROCEDURES(20),
  CLI_CURSOR_COMMIT_BEHAVIOR(23),
  CLI_DATA_SOURCE_READ_ONLY(25),
  CLI_DEFAULT_TXN_ISOLATION(26),
  CLI_IDENTIFIER_CASE(28),
  CLI_IDENTIFIER_QUOTE_CHAR(29),
  CLI_MAX_COLUMN_NAME_LEN(30),
  CLI_MAX_CURSOR_NAME_LEN(31),
  CLI_MAX_SCHEMA_NAME_LEN(32),
  CLI_MAX_CATALOG_NAME_LEN(34),
  CLI_MAX_TABLE_NAME_LEN(35),
  CLI_SCROLL_CONCURRENCY(43),
  CLI_TXN_CAPABLE(46),
  CLI_USER_NAME(47),
  CLI_TXN_ISOLATION_OPTION(72),
  CLI_INTEGRITY(73),
  CLI_GETDATA_EXTENSIONS(81),
  CLI_NULL_COLLATION(85),
  CLI_ALTER_TABLE(86),
  CLI_ORDER_BY_COLUMNS_IN_SELECT(90),
  CLI_SPECIAL_CHARACTERS(94),
  CLI_MAX_COLUMNS_IN_GROUP_BY(97),
  CLI_MAX_COLUMNS_IN_INDEX(98),
  CLI_MAX_COLUMNS_IN_ORDER_BY(99),
  CLI_MAX_COLUMNS_IN_SELECT(100),
  CLI_MAX_COLUMNS_IN_TABLE(101),
  CLI_MAX_INDEX_SIZE(102),
  CLI_MAX_ROW_SIZE(104),
  CLI_MAX_STATEMENT_LEN(105),
  CLI_MAX_TABLES_IN_SELECT(106),
  CLI_MAX_USER_NAME_LEN(107),
  CLI_OJ_CAPABILITIES(115),
  CLI_XOPEN_CLI_YEAR(10000),
  CLI_CURSOR_SENSITIVITY(10001),
  CLI_DESCRIBE_PARAMETER(10002),
  CLI_CATALOG_NAME(10003),
  CLI_COLLATION_SEQ(10004),
  CLI_MAX_IDENTIFIER_LEN(10005),
  CLI_ODBC_KEYWORDS(10006);

  private final int value;

  private TGetInfoType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  @org.apache.thrift.annotation.Nullable
  public static TGetInfoType findByValue(int value) { 
    switch (value) {
      case 0:
        return CLI_MAX_DRIVER_CONNECTIONS;
      case 1:
        return CLI_MAX_CONCURRENT_ACTIVITIES;
      case 2:
        return CLI_DATA_SOURCE_NAME;
      case 8:
        return CLI_FETCH_DIRECTION;
      case 13:
        return CLI_SERVER_NAME;
      case 14:
        return CLI_SEARCH_PATTERN_ESCAPE;
      case 17:
        return CLI_DBMS_NAME;
      case 18:
        return CLI_DBMS_VER;
      case 19:
        return CLI_ACCESSIBLE_TABLES;
      case 20:
        return CLI_ACCESSIBLE_PROCEDURES;
      case 23:
        return CLI_CURSOR_COMMIT_BEHAVIOR;
      case 25:
        return CLI_DATA_SOURCE_READ_ONLY;
      case 26:
        return CLI_DEFAULT_TXN_ISOLATION;
      case 28:
        return CLI_IDENTIFIER_CASE;
      case 29:
        return CLI_IDENTIFIER_QUOTE_CHAR;
      case 30:
        return CLI_MAX_COLUMN_NAME_LEN;
      case 31:
        return CLI_MAX_CURSOR_NAME_LEN;
      case 32:
        return CLI_MAX_SCHEMA_NAME_LEN;
      case 34:
        return CLI_MAX_CATALOG_NAME_LEN;
      case 35:
        return CLI_MAX_TABLE_NAME_LEN;
      case 43:
        return CLI_SCROLL_CONCURRENCY;
      case 46:
        return CLI_TXN_CAPABLE;
      case 47:
        return CLI_USER_NAME;
      case 72:
        return CLI_TXN_ISOLATION_OPTION;
      case 73:
        return CLI_INTEGRITY;
      case 81:
        return CLI_GETDATA_EXTENSIONS;
      case 85:
        return CLI_NULL_COLLATION;
      case 86:
        return CLI_ALTER_TABLE;
      case 90:
        return CLI_ORDER_BY_COLUMNS_IN_SELECT;
      case 94:
        return CLI_SPECIAL_CHARACTERS;
      case 97:
        return CLI_MAX_COLUMNS_IN_GROUP_BY;
      case 98:
        return CLI_MAX_COLUMNS_IN_INDEX;
      case 99:
        return CLI_MAX_COLUMNS_IN_ORDER_BY;
      case 100:
        return CLI_MAX_COLUMNS_IN_SELECT;
      case 101:
        return CLI_MAX_COLUMNS_IN_TABLE;
      case 102:
        return CLI_MAX_INDEX_SIZE;
      case 104:
        return CLI_MAX_ROW_SIZE;
      case 105:
        return CLI_MAX_STATEMENT_LEN;
      case 106:
        return CLI_MAX_TABLES_IN_SELECT;
      case 107:
        return CLI_MAX_USER_NAME_LEN;
      case 115:
        return CLI_OJ_CAPABILITIES;
      case 10000:
        return CLI_XOPEN_CLI_YEAR;
      case 10001:
        return CLI_CURSOR_SENSITIVITY;
      case 10002:
        return CLI_DESCRIBE_PARAMETER;
      case 10003:
        return CLI_CATALOG_NAME;
      case 10004:
        return CLI_COLLATION_SEQ;
      case 10005:
        return CLI_MAX_IDENTIFIER_LEN;
      case 10006:
        return CLI_ODBC_KEYWORDS;
      default:
        return null;
    }
  }
}
