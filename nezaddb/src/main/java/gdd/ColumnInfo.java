package gdd;

import globalDefinition.CONSTANT;

public class ColumnInfo {
    private String columnName;
    private int tableID;
    private int columnID;
    private int columnType;
    private int nullable;
    private int keyable;
    private int maxLength;

    public ColumnInfo() {

    }

    public ColumnInfo(String name, int id, int type, int nullable, int keyable,
            int length) {
        this.columnName = name;
        this.columnID = id;
        this.columnType = type;
        this.nullable = nullable;
        this.keyable = keyable;
        this.maxLength = length;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public int getTableID() {
        return this.tableID;
    }

    public int getColumnID() {
        return this.columnID;
    }

    public int getColumnType() {
        return this.columnType;
    }

    public String getColumnTypeName() {
        switch (columnType) {
        case CONSTANT.VALUE_INT:
            return "integer";
        case CONSTANT.VALUE_STRING:
            return "char("+ maxLength +")";
        case CONSTANT.VALUE_DOUBLE:
            return "float";
        default:
            break;
        }
        return "integer";
    }

    public int getColumnNullable() {
        return this.nullable;
    }

    public int getColumnKeyable() {
        return this.keyable;
    }

    public int getColumnLength() {
        return this.maxLength;
    }

    public void printColumnInfo() {
        System.out.print("columnName=" + this.columnName);
        System.out.print(", columnID=" + this.columnID);
        System.out.print(", columnType=" + this.columnType);
        System.out.print(", nullable=" + this.nullable);
        System.out.print(", keyable=" + this.keyable);
        System.out.print(", maxLength=" + this.maxLength);
        System.out.println();

    }
}