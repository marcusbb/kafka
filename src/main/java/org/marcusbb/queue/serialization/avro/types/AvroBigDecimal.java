package org.marcusbb.queue.serialization.avro.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class AvroBigDecimal {

	private int scaled;
	private ByteBuffer unscaled;
	
		
	public AvroBigDecimal(BigDecimal bd) {

		this.scaled = bd.scale();
		
		this.unscaled = ByteBuffer.wrap( bd.unscaledValue().toByteArray() );
	}
	public AvroBigDecimal(int i) {
		this(new BigDecimal(i));
	}
	public AvroBigDecimal(double d) {
		this(new BigDecimal(d));
	}
	public AvroBigDecimal(String str) {
		this(new BigDecimal(str));
	}
	public AvroBigDecimal() {}

	public BigDecimal asBigDecimal() {
		return new BigDecimal(new BigInteger(unscaled.array()),scaled);
	}
	public int getScaled() {
		return scaled;
	}
	public void setScaled(int scaled) {
		this.scaled = scaled;
	}
	public ByteBuffer getUnscaled() {
		return unscaled;
	}
	public void setUnscaled(ByteBuffer unscaled) {
		this.unscaled = unscaled;
	}

}
