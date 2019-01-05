package org.marcusbb.queue.kafka.utils;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class TestQueueItem {

	private String sourceRegion;
	private String destinationRegion;
	private String msgCode;
	private String batchJobCode;
	private Integer type;
	private String reference;
	private String cardNumber;
	private String virtualCardNumber;
	private Timestamp authorizationTS;
	private String descriptor;
	private String mcc;
	private Short pos;
	private String posEntry;
	private BigDecimal amount;
	private String currency;
	private String liability;
	private String initiatedBy;
	private String merchantId;
	private Timestamp flaggedTS;
	private Timestamp settledTS;
	private String authCode;
	private String authDecision;
	private String acquirerId;
	private String arn;
	private Short arnUpdateCode;
	private String invalidArn;
	private String eciIndicator;
	private String eciIndicatorDescr;
	private String chargebackReasonCode;
	private BigDecimal chargebackAmount;
	private String chargebackCurrency;
	private String transactionId;
	private Timestamp currentTS;
	private String timeZone;
	private boolean testModeEnabledEvent;
	private Integer providerId;
	private String dataInterface;
	private Integer memberInterfaceModeId;
	private String cardholderAuthenticationCode;
	private Integer memberId;
	private Integer userId;
	private Long batchJobId;
	private Integer matchId;
	private String comments;
	private Short memberMatchStatus;
	private Long memberMatchId;
	private String alertSource;
	private String sourceId;
	private String boaBusiness;

	private String visaSource;
	private String visaMid;
	private String visaAmount;
	private String visaMerchant;
	private String visaNotificationCode;
	private String merchantFullDescriptor;

	/*private String cardBIN;
 	private String cardLast4;
	private String cardFormat;
 	private String cardScheme;*/

	public String getSourceRegion() {
		return sourceRegion;
	}

	public void setSourceRegion(String sourceRegion) {
		this.sourceRegion = sourceRegion;
	}

	public String getDestinationRegion() {
		return destinationRegion;
	}

	public void setDestinationRegion(String destinationRegion) {
		this.destinationRegion = destinationRegion;
	}

	

	public String getMsgCode() {
		return msgCode;
	}

	public void setMsgCode(String msgCode) {
		this.msgCode = msgCode;
	}

	public String getBatchJobCode() {
		return batchJobCode;
	}

	public void setBatchJobCode(String batchJobCode) {
		this.batchJobCode = batchJobCode;
	}

	public String getVisaSource() {
		return visaSource;
	}

	public void setVisaSource(String visaSource) {
		this.visaSource = visaSource;
	}

	public String getVisaMid() {
		return visaMid;
	}

	public String getVisaAmount() {
		return visaAmount;
	}

	public String getVisaMerchant() {
		return visaMerchant;
	}

	public String getVisaNotificationCode() {
		return visaNotificationCode;
	}

	public void setVisaMid(String visaMid) {
		this.visaMid = visaMid;
	}

	public void setVisaAmount(String visaAmount) {
		this.visaAmount = visaAmount;
	}

	public void setVisaMerchant(String visaMerchant) {
		this.visaMerchant = visaMerchant;
	}

	public void setVisaNotificationCode(String visaNotificationCode) {
		this.visaNotificationCode = visaNotificationCode;
	}

	public String getBOABusiness() {
		return boaBusiness;
	}

	public void setBOABusiness(String boaBusiness) {
		this.boaBusiness = boaBusiness;
	}

	public String getAlertSource() {
		return alertSource;
	}

	public void setAlertSource(String alertSource) {
		this.alertSource = alertSource;
	}

	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	public Integer getType() {
		return type;
	}

	public void setType(Integer type) {
		this.type = type;
	}

	public String getReference() {
		return reference;
	}

	public void setReference(String reference) {
		this.reference = reference;
	}

	public String getCardNumber() {
		return cardNumber;
	}

	public void setCardNumber(String cardNumber) {
		this.cardNumber = cardNumber;
	}

	public String getVirtualCardNumber() {
		return virtualCardNumber;
	}

	public void setVirtualCardNumber(String virtualCardNumber) {
		this.virtualCardNumber = virtualCardNumber;
	}

	public Timestamp getAuthorizationTS() {
		return authorizationTS;
	}

	public void setAuthorizationTS(Timestamp authorizationTS) {
		this.authorizationTS = authorizationTS;
	}

	public String getDescriptor() {
		return descriptor;
	}

	public void setDescriptor(String descriptor) {
		this.descriptor = descriptor;
	}

	public String getMcc() {
		return mcc;
	}

	public void setMcc(String mcc) {
		this.mcc = mcc;
	}

	public Short getPos() {
		return pos;
	}

	public void setPos(Short pos) {
		this.pos = pos;
	}

	public String getPosEntry() {
		return posEntry;
	}

	public void setPosEntry(String posEntry) {
		this.posEntry = posEntry;
	}

	public BigDecimal getAmount() {
		return amount;
	}

	public void setAmount(BigDecimal amount) {
		this.amount = amount;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public String getLiability() {
		return liability;
	}

	public void setLiability(String liability) {
		this.liability = liability;
	}

	public String getInitiatedBy() {
		return initiatedBy;
	}

	public void setInitiatedBy(String initiatedBy) {
		this.initiatedBy = initiatedBy;
	}

	public String getMerchantId() {
		return merchantId;
	}

	public void setMerchantId(String merchantId) {
		this.merchantId = merchantId;
	}

	public Timestamp getFlaggedTS() {
		return flaggedTS;
	}

	public void setFlaggedTS(Timestamp flaggedTS) {
		this.flaggedTS = flaggedTS;
	}

	public Timestamp getSettledTS() {
		return settledTS;
	}

	public void setSettledTS(Timestamp settledTS) {
		this.settledTS = settledTS;
	}

	public String getAuthCode() {
		return authCode;
	}

	public void setAuthCode(String authCode) {
		this.authCode = authCode;
	}

	public String getAuthDecision() {
		return authDecision;
	}

	public void setAuthDecision(String authDecision) {
		this.authDecision = authDecision;
	}

	public String getAcquirerId() {
		return acquirerId;
	}

	public void setAcquirerId(String acquirerId) {
		this.acquirerId = acquirerId;
	}

	public String getArn() {
		return arn;
	}

	public void setArn(String arn) {
		this.arn = arn;
	}

	public Short getArnUpdateCode() {
		return arnUpdateCode;
	}

	public void setArnUpdateCode(Short arnUpdateCode) {
		this.arnUpdateCode = arnUpdateCode;
	}

	public String getInvalidArn() {
		return invalidArn;
	}

	public void setInvalidArn(String invalidArn) {
		this.invalidArn = invalidArn;
	}

	public String getEciIndicator() {
		return eciIndicator;
	}

	public void setEciIndicator(String eciIndicator) {
		this.eciIndicator = eciIndicator;
	}

	public String getEciIndicatorDescr() {
		return eciIndicatorDescr;
	}

	public void setEciIndicatorDescr(String eciIndicatorDescr) {
		this.eciIndicatorDescr = eciIndicatorDescr;
	}

	public String getChargebackReasonCode() {
		return chargebackReasonCode;
	}

	public void setChargebackReasonCode(String chargebackReasonCode) {
		this.chargebackReasonCode = chargebackReasonCode;
	}

	public BigDecimal getChargebackAmount() {
		return chargebackAmount;
	}

	public void setChargebackAmount(BigDecimal chargebackAmount) {
		this.chargebackAmount = chargebackAmount;
	}

	public String getChargebackCurrency() {
		return chargebackCurrency;
	}

	public void setChargebackCurrency(String chargebackCurrency) {
		this.chargebackCurrency = chargebackCurrency;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public Timestamp getCurrentTS() {
		return currentTS;
	}

	public void setCurrentTS(Timestamp currentTS) {
		this.currentTS = currentTS;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}

	public boolean isTestModeEnabledEvent() {
		return testModeEnabledEvent;
	}

	public void setTestModeEnabledEvent(boolean testModeEnabledEvent) {
		this.testModeEnabledEvent = testModeEnabledEvent;
	}

	public Integer getProviderId() {
		return providerId;
	}

	public void setProviderId(Integer providerId) {
		this.providerId = providerId;
	}

	public String getDataInterface() {
		return dataInterface;
	}

	public void setDataInterface(String dataInterface) {
		this.dataInterface = dataInterface;
	}

	public Integer getMemberInterfaceModeId() {
		return memberInterfaceModeId;
	}

	public void setMemberInterfaceModeId(Integer memberInterfaceModeId) {
		this.memberInterfaceModeId = memberInterfaceModeId;
	}

	public String getCardholderAuthenticationCode() {
		return cardholderAuthenticationCode;
	}

	public void setCardholderAuthenticationCode(String cardholderAuthenticationCode) {
		this.cardholderAuthenticationCode = cardholderAuthenticationCode;
	}

	public Integer getMemberId() {
		return memberId;
	}

	public void setMemberId(Integer memberId) {
		this.memberId = memberId;
	}

	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public Long getBatchJobId() {
		return batchJobId;
	}

	public void setBatchJobId(Long batchJobId) {
		this.batchJobId = batchJobId;
	}

	public String getComments() {
		return comments;
	}

	public void setComments(String comments) {
		this.comments = comments;
	}

	public Integer getMatchId() {
		return matchId;
	}

	public void setMatchId(Integer matchId) {
		this.matchId = matchId;
	}

	public Long getMemberMatchId() {
		return memberMatchId;
	}

	public void setMemberMatchId(Long memberMatchId) {
		this.memberMatchId = memberMatchId;
	}

	public void setMemberMatchStatus(Short memberMatchStatus) {
		this.memberMatchStatus = memberMatchStatus;
	}

	public Short getMemberMatchStatus() {
		return memberMatchStatus;
	}

	public String getMerchantFullDescriptor() {
		return merchantFullDescriptor;
	}

	public void setMerchantFullDescriptor(String merchantFullDescriptor) {
		this.merchantFullDescriptor = merchantFullDescriptor;
	}

/*	public String getCardBIN() {
		return cardBIN;
	}

	public void setCardBIN(String cardBIN) {
		this.cardBIN = cardBIN;
	}

	public String getCardLast4() {
		return cardLast4;
	}

	public void setCardLast4(String cardLast4) {
		this.cardLast4 = cardLast4;
	}

	public String getCardScheme() {
		return cardScheme;
	}

	public void setCardScheme(String cardScheme) {
		this.cardScheme = cardScheme;
	}

	public String getCardFormat() {
		return cardFormat;
	}

	public void setCardFormat(String cardFormat) {
		this.cardFormat = cardFormat;
	}*/
}
