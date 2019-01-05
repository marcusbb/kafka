package org.marcusbb.queue.kafka.utils;

import java.io.Serializable;

/**
 * TestMessage with an added field: moreContent
 * Used for testing schema evolution.
 */
public class TestMessageV2 implements Serializable {

	String content;
	String moreContent;

	public TestMessageV2() {}

	public TestMessageV2(String content){
		this.content = content;
		this.moreContent = null;
	}

	public TestMessageV2(String content, String moreContent) {
		this.content = content;
		this.moreContent = moreContent;
	}

	public String getContent() {
		return content;
	}

	public String getMoreContent() {
		return moreContent;
	}

	@Override
	public String toString() {
		return "TestMessageV2 [content=" + content + ", moreContent=" + moreContent + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TestMessageV2 that = (TestMessageV2) o;

		return content.equals(that.content) && moreContent.equals(that.moreContent);
	}
}
