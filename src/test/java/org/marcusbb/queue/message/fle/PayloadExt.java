package org.marcusbb.queue.message.fle;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Map;

import javax.ws.rs.DefaultValue;

import org.apache.avro.reflect.Nullable;
import org.marcusbb.crypto.reflect.EncryptedField;
import org.marcusbb.queue.AbstractREM;

public class PayloadExt extends AbstractREM {

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



    public String getCreditCard() {
        return creditCard;
    }

    public void setCreditCard(String creditCard) {
        this.creditCard = creditCard;
    }

    public PayloadExt() {}

    public PayloadExt(Map<String, String> headers) {
        super(headers);
    }

    public PayloadExt(String str) {
        this.firstString = str;
    }

    public PayloadExt(String creditCard, BigDecimal bigDecimal){
        this.creditCard = creditCard;
        this.bigDecimal1 = bigDecimal;
    }

    public PayloadExt( BigDecimal bigDecimal1, String firstString,Integer firstInt,Long firstLong) {
        super();

        this.bigDecimal1 = bigDecimal1;
        this.firstString = firstString;
        this.firstInt = firstInt;
        this.firstLong = firstLong;
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
}