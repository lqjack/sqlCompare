/*
 * Copyright 2013 <a href="mailto:onacit@gmail.com">Jin Kwon</a>.
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


import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import static java.lang.invoke.MethodHandles.lookup;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import static java.sql.DriverManager.getConnection;
import java.sql.SQLException;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 *
 * @author Jin Kwon &lt;jinahya_at_gmail.com&gt;
 */
public class H2MemoryTest {


    private static final Logger logger = getLogger(lookup().lookupClass());


    private static final String DRIVER_NAME = "org.h2.Driver";


    private static final String CONNECTION_URL
        = "jdbc:h2:mem:test"; //;DB_CLOSE_DELAY=-1";


    @BeforeClass
    private static void beforeClass() throws SQLException {
    }


    @AfterClass
    private static void afterClass() throws SQLException {
    }


    @Test(enabled = true)
    public void retrieve() throws Exception {

        final Metadata metadata;

        try (Connection connection = getConnection(CONNECTION_URL)) {
            final DatabaseMetaData database = connection.getMetaData();
            final MetadataContext context = new MetadataContext(database);
            context.addSuppressions(
                "column/isGeneratedcolumn",
                "metadata/generatedKeyAlwaysReturned",
                "schema/functions",
                "table/pseudoColumns"
            );
            metadata = context.getMetadata();
        }

        final JAXBContext context = JAXBContext.newInstance(Metadata.class);
        final Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        final File file = new File("target", "h2.memory.metadata.xml");
        try (OutputStream outputStream = new FileOutputStream(file)) {
            marshaller.marshal(metadata, outputStream);
            outputStream.flush();
        }
    }

}

