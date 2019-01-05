package org.marcusbb.queue.message.fle;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

import javax.ws.rs.DefaultValue;

import org.apache.avro.reflect.Nullable;
import org.marcusbb.crypto.reflect.EncryptedField;
import org.marcusbb.queue.AbstractREM;
import org.marcusbb.queue.serialization.AvroSchemaVersion;

/**
 * Version 3 of PayloadExt test class; used to test schema-evolution
 */
@AvroSchemaVersion(value=3, subject="PayloadExt")
public class PayloadExtV3 extends AbstractREM {

    @Nullable
    private BigDecimal bigDecimal1;

    @Nullable
    private String firstString;

    @DefaultValue(value = "")
    private String notNullable= "" ;

    @Nullable
    private Integer firstInt;


    private Long firstLong;

    @Nullable
    private Timestamp date;
    @Nullable
    private String appendedv2;

    @EncryptedField(alias = "CreditCard", iv = "iv1")
    private String creditCard;

    private String oof = "bar";

    @EncryptedField(alias = "SecretOof", iv = "iv1")
    private String secretOof;

    @EncryptedField(alias = "AltSecret", iv = "iv1")
    private String altSecret;

    public String getCreditCard() {
        return creditCard;
    }

    public void setCreditCard(String creditCard) {
        this.creditCard = creditCard;
    }

    public PayloadExtV3() {}

    public PayloadExtV3(Map<String, String> headers) {
        super(headers);
        // TODO Auto-generated constructor stub
    }

    public PayloadExtV3(String str) {
        this.firstString = str;
    }
    public PayloadExtV3( BigDecimal bigDecimal1, String firstString,Integer firstInt,Long firstLong) {
        super();

        this.bigDecimal1 = bigDecimal1;
        this.firstString = firstString;
        this.firstInt = firstInt;
        this.firstLong = firstLong;
    }

    public PayloadExtV3 (BigDecimal bigDecimal1, String firstString, Integer firstInt, Long firstLong, String oof) {
        super();
        this.bigDecimal1 = bigDecimal1;
        this.firstString = firstString;
        this.firstInt = firstInt;
        this.firstLong = firstLong;
        this.oof = oof;
    }

    public Integer getFirstInt() {
        return firstInt;
    }

    public void setFirstInt(Integer firstInt) {
        this.firstInt = firstInt;
    }

    public BigDecimal getBigDecimal1() {
        return bigDecimal1;
    }

    public void setBigDecimal1(BigDecimal bigDecimal1) {
        this.bigDecimal1 = bigDecimal1;
    }

    public String getFirstString() {
        return firstString;
    }

    public void setFirstString(String firstString) {
        this.firstString = firstString;
    }

    public Long getFirstLong() {
        return firstLong;
    }

    public void setFirstLong(Long firstLong) {
        this.firstLong = firstLong;
    }

    public String getNotNullable() {
        return notNullable;
    }

    public void setNotNullable(String notNullable) {
        this.notNullable = notNullable;
    }

    public Timestamp getDate() {
        return date;
    }

    public void setDate(Timestamp date) {
        this.date = date;
    }
    public String getAppendedv2() {
        return appendedv2;
    }

    public void setAppendedv2(String appendedv2) {
        this.appendedv2 = appendedv2;
    }

    public String getOof() {
        return oof;
    }

    public void setOof(String oof) {
        this.oof = oof;
    }

    public String getSecretOof() {
        return secretOof;
    }

    public void setSecretOof(String secretOof) {
        this.secretOof = secretOof;
    }

    public String getAltSecret() {
        return altSecret;
    }

    public void setAltSecret(String altSecret) {
        this.altSecret = altSecret;
    }
}

