/*
 * Copyright 2011 Jin Kwon <jinahya at gmail.com>.
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
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.commons.lang3.builder.CompareToBuilder;


/**
 * An entity class for binding the result of
 * {@link java.sql.DatabaseMetaData#getSchemas()}.
 *
 * @author Jin Kwon &lt;jinahya_at_gmail.com&gt;
 */
@XmlRootElement
@XmlType(
    propOrder = {
        "tableSchem", "tableCatalog"
    }
)
public class SchemaName extends AbstractChild<Metadata> {


    public static Comparator<SchemaName> natural() {

        return new Comparator<SchemaName>() {

            @Override
            public int compare(final SchemaName o1, final SchemaName o2) {

                // by TABLE_CATALOG and TABLE_SCHEM
                return new CompareToBuilder()
                    .append(o1.getTableCatalog(), o2.getTableCatalog())
                    .append(o1.getTableSchem(), o2.getTableSchem())
                    .build();
            }

        };
    }


    @Override
    public String toString() {

        return super.toString() + "{"
               + "tableSchem=" + tableSchem
               + ", tableCatalog=" + tableCatalog
               + "}";
    }


    // -------------------------------------------------------------- tableSchem
    public String getTableSchem() {

        return tableSchem;
    }


    public void setTableSchem(final String tableSchem) {

        this.tableSchem = tableSchem;
    }


    // ------------------------------------------------------------ tableCatalog
    public String getTableCatalog() {

        return tableCatalog;
    }


    public void setTableCatalog(final String tableCatalog) {

        this.tableCatalog = tableCatalog;
    }


    // ---------------------------------------------------------------- metadata
    // just for class diagram
    private Metadata getMetadata() {

        return getParent();
    }


//    public void setMetadata(final Metadata metadata) {
//
//        setParent(metadata);
//    }
//
//
//    SchemaName metadata(final Metadata metadata) {
//
//        setMetadata(metadata);
//
//        return this;
//    }
    // -------------------------------------------------------------------------
    @Label("TABLE_SCHEM")
    @XmlElement(required = true)
    private String tableSchem;


    @Label("TABLE_CATALOG")
    @NillableBySpecification
    @XmlElement(nillable = true, required = false)
    private String tableCatalog;

}

