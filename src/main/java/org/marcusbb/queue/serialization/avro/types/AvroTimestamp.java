package org.marcusbb.queue.serialization.avro.types;

import java.io.Serializable;
import java.sql.Timestamp;

public class AvroTimestamp implements Serializable{

	private long epochLong;
	
	
	
	public AvroTimestamp(Timestamp ts) {
		
		this.epochLong = ts.getTime();
	}
	public AvroTimestamp(long ts) {
		
		this(new Timestamp(ts));
	}
	
	public AvroTimestamp() {}
	
	public long getEpochLong() {
		return epochLong;
	}
	public void setEpochLong(long epochLong) {
		this.epochLong = epochLong;
	}
	public Timestamp asTs() {
		return new Timestamp(epochLong);
	}
	
	
	
}
