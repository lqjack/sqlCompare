/*
 * Copyright 2013 Jin Kwon <onacit at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.github.jinahya.sql.database.metadata.bind;


import java.util.Comparator;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.commons.lang3.builder.CompareToBuilder;


/**
 * An entity class for binding the result of
 * {@link java.sql.DatabaseMetaData#getFunctionColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)}.
 *
 * @author Jin Kwon &lt;jinahya_at_gmail.com&gt;
 */
@XmlRootElement
@XmlType(
    propOrder = {
        "functionName", "columnName", "columnType", "dataType", "typeName",
        "precision", "length", "scale", "radix", "nullable", "remarks",
        "charOctetLength", "ordinalPosition", "isNullable", "specificName"
    }
)
public class FunctionColumn extends AbstractChild<Function> {


    public static Comparator<FunctionColumn> natural() {

        return new Comparator<FunctionColumn>() {

            @Override
            public int compare(final FunctionColumn o1,
                               final FunctionColumn o2) {

                // by FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME
                // and SPECIFIC_NAME.
                return new CompareToBuilder()
                    .append(o1.getFunctionCat(), o2.getFunctionCat())
                    .append(o1.getFunctionSchem(), o2.getFunctionSchem())
                    .append(o1.getFunctionName(), o2.getFunctionName())
                    .append(o1.getSpecificName(), o2.getSpecificName())
                    .build();
            }

        };
    }


    @Override
    public String toString() {

        return super.toString() + "{"
               + "functionCat=" + functionCat
               + ", functionSchem=" + functionSchem
               + ", functionName=" + functionName
               + ", columnName=" + columnName
               + ", columnType=" + columnType
               + ", dataType=" + dataType
               + ", typeName=" + typeName
               + ", precision=" + precision
               + ", length=" + length
               + ", scale=" + scale
               + ", radix=" + radix
               + ", nullable=" + nullable
               + ", remarks=" + remarks
               + ", charOctetLength=" + charOctetLength
               + ", ordinalPosition=" + ordinalPosition
               + ", isNullable=" + isNullable
               + ", specificName=" + specificName
               + "}";
    }


    // ------------------------------------------------------------- functionCat
    public String getFunctionCat() {

        return functionCat;
    }


    public void setFunctionCat(final String functionCat) {

        this.functionCat = functionCat;
    }


    // ----------------------------------------------------------- functionSchem
    public String getFunctionSchem() {

        return functionSchem;
    }


    public void setFunctionSchem(final String functionSchem) {

        this.functionSchem = functionSchem;
    }


    // ------------------------------------------------------------ functionName
    public String getFunctionName() {

        return functionName;
    }


    public void setFunctionName(final String functionName) {

        this.functionName = functionName;
    }


    // -------------------------------------------------------------- columnName
    public String getColumnName() {

        return columnName;
    }


    public void setColumnName(final String columnName) {

        this.columnName = columnName;
    }


    // -------------------------------------------------------------- columnType
    public short getColumnType() {

        return columnType;
    }


    public void setColumnType(final short columnType) {

        this.columnType = columnType;
    }


    // ---------------------------------------------------------------- dataType
    public int getDataType() {

        return dataType;
    }


    public void setDataType(final int dataType) {

        this.dataType = dataType;
    }


    // ---------------------------------------------------------------- typeName
    public String getTypeName() {

        return typeName;
    }


    public void setTypeName(final String typeName) {

        this.typeName = typeName;
    }


    // --------------------------------------------------------------- precision
    public int getPrecision() {

        return precision;
    }


    public void setPrecision(final int precision) {

        this.precision = precision;
    }


    // ------------------------------------------------------------------ length
    public int getLength() {

        return length;
    }


    public void setLength(final int length) {

        this.length = length;
    }


    // ------------------------------------------------------------------- scale
    public Short getScale() {

        return scale;
    }


    public void setScale(final Short scale) {

        this.scale = scale;
    }


    // ------------------------------------------------------------------- radix
    public short getRadix() {

        return radix;
    }


    public void setRadix(final short radix) {

        this.radix = radix;
    }


    // ---------------------------------------------------------------- nullable
    public short getNullable() {

        return nullable;
    }


    public void setNullable(final short nullable) {

        this.nullable = nullable;
    }


    // ----------------------------------------------------------------- remarks
    public String getRemarks() {

        return remarks;
    }


    public void setRemarks(final String remarks) {

        this.remarks = remarks;
    }


    // --------------------------------------------------------- charOctetLength
    public Integer getCharOctetLength() {

        return charOctetLength;
    }


    public void setCharOctetLength(final Integer charOctetLength) {

        this.charOctetLength = charOctetLength;
    }


    // --------------------------------------------------------- ordinalPosition
    public int getOrdinalPosition() {

        return ordinalPosition;
    }


    public void setOrdinalPosition(final int ordinalPosition) {

        this.ordinalPosition = ordinalPosition;
    }


    // -------------------------------------------------------------- isNullable
    public String getIsNullable() {

        return isNullable;
    }


    public void setIsNullable(final String isNullable) {

        this.isNullable = isNullable;
    }


    // ------------------------------------------------------------ specificName
    public String getSpecificName() {

        return specificName;
    }


    public void setSpecificName(final String specificName) {

        this.specificName = specificName;
    }


    // ---------------------------------------------------------------- function
    public Function getFunction() {

        return getParent();
    }


    public void setFunction(final Function function) {

        setParent(function);
    }


    // -------------------------------------------------------------------------
    @Label("FUNCTION_CAT")
    @NillableBySpecification
    @XmlAttribute
    private String functionCat;


    @Label("FUNCTION_SCHEM")
    @NillableBySpecification
    @XmlAttribute
    private String functionSchem;


    @Label("FUNCTION_NAME")
    @XmlAttribute
    private String functionName;


    @Label("COLUMN_NAME")
    @XmlElement(required = true)
    private String columnName;


    @Label("COLUMN_TYPE")
    @XmlElement(required = true)
    private short columnType;


    @Label("DATA_TYPE")
    @XmlElement(required = true)
    private int dataType;


    @Label("TYPE_NAME")
    @XmlElement(required = true)
    private String typeName;


    @Label("PRECISION")
    @XmlElement(required = true)
    private int precision;


    @Label("LENGTH")
    @XmlElement(required = true)
    private int length;


    @Label("SCALE")
    @XmlElement(required = true)
    private Short scale;


    @Label("RADIX")
    @XmlElement(required = true)
    private short radix;


    @Label("NULLABLE")
    @XmlElement(required = true)
    private short nullable;


    @Label("REMARKS")
    @XmlElement(required = true)
    private String remarks;


    @Label("CHAR_OCTET_LENGTH")
    @XmlElement(required = true)
    @NillableBySpecification
    private Integer charOctetLength;


    @Label("ORDINAL_POSITION")
    @XmlElement(required = true)
    private int ordinalPosition;


    @Label("IS_NULLABLE")
    @XmlElement(required = true)
    private String isNullable;


    @Label("SPECIFIC_NAME")
    @XmlElement(required = true)
    private String specificName;

}

