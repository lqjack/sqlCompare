/*
 * Copyright 2015 Jin Kwon &lt;jinahya_at_gmail.com&gt;.
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


import java.util.logging.Level;
import java.util.logging.Logger;
import static java.util.logging.Logger.getLogger;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;


/**
 *
 * @author Jin Kwon &lt;jinahya_at_gmail.com&gt;
 */
class RSTRSCBoolean {


    private static final Logger logger
        = getLogger(RSTRSCBoolean.class.getName());


    static RSTRSCBoolean valueOf(final Object[] args, final Object value) {

        final RSTRSCBoolean instance = new RSTRSCBoolean();

        instance.setType((Integer) args[0]);
        instance.setConcurrency((Integer) args[1]);
        instance.setValue((Boolean) value);

        return instance;
    }


    // -------------------------------------------------------------------- type
    public int getType() {

        return type;
    }


    public void setType(final int type) {

        this.type = type;
    }


    RSTRSCBoolean type(final int type) {

        setType(type);

        return this;
    }


    @XmlAttribute
    public String getTypeName() {

        try {
            return RST.valueOf(type).name();
        } catch (final IllegalArgumentException iae) {
            logger.log(Level.WARNING, "unknown result set type: {0}",
                       new Object[]{type});
        }

        return null;
    }


    // ------------------------------------------------------------- concurrency
    public int getConcurrency() {

        return concurrency;
    }


    public void setConcurrency(final int concurrency) {

        this.concurrency = concurrency;
    }


    @XmlAttribute
    public String getConccurrencyName() {

        try {
            return RSC.valueOf(concurrency).name();
        } catch (final IllegalArgumentException iae) {
            logger.log(Level.WARNING, "unknown result set concurrency: {0}",
                       new Object[]{concurrency});
        }

        return null;
    }


    // ------------------------------------------------------------------- value
    public boolean getValue() {

        return value;
    }


    public void setValue(final boolean value) {

        this.value = value;
    }


    RSTRSCBoolean value(final boolean value) {

        setValue(value);

        return this;
    }


    // -------------------------------------------------------------------------
    @XmlAttribute
    private int type;


    @XmlAttribute
    private int concurrency;


    @XmlValue
    private boolean value;

}

