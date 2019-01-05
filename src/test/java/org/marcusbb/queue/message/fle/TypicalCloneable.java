package org.marcusbb.queue.message.fle;

import java.math.BigDecimal;
import java.sql.Timestamp;

import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.marcusbb.crypto.reflect.EncryptedField;
import org.marcusbb.queue.AbstractREM;

public class TypicalCloneable extends AbstractREM {

    @EncryptedField(iv="IVCardNumber", alias="cardnumber")
    private String cardNumber = "9999";

    @AvroName(value="myTs")
    private Timestamp myTs;

    @Nullable
    private BigDecimal myBigDecimal;


    public TypicalCloneable() {}
    public TypicalCloneable(String cc) {
        this.cardNumber = cc;
    }
    public TypicalCloneable(String cc,Timestamp ts) {
        this.cardNumber = cc;
        this.myTs = ts;
    }
    public String getCardNumber() {
        return cardNumber;
    }

    public void setCardNumber(String cardNumber) {
        this.cardNumber = cardNumber;
    }


    public Timestamp getMyTs() {
        return myTs;
    }
    public void setMyTs(Timestamp ts) {
        this.myTs = ts;
    }
    public BigDecimal getMyBigDecimal() {
        return myBigDecimal;
    }
    public void setMyBigDecimal(BigDecimal myBigDecimal) {
        this.myBigDecimal = myBigDecimal;
    }

}
