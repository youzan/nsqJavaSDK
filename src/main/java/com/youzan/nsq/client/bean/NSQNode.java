package com.youzan.nsq.client.bean;


/**
 * nsqd bean
 * @author maoxiajun
 *
 */
public class NSQNode {
	private String host;
	private int port;
	//private String workingstat;
	
	public NSQNode(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		} else if (null == obj) {
			return false;
		} else if (!(obj instanceof NSQNode)) {
			return false;
		} else {
			NSQNode node = (NSQNode) obj;
			if (((host == node.getHost()) ||
				(host != null && host.equals(node.getHost()))) &&
				port == node.getPort()) {
				return true;
			}
		}
		
		return false;
	}
	
	public String getHost() {
		return host;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public int getPort() {
		return port;
	}
	
	public void setPort(int port) {
		this.port = port;
	}

	/*public String getWorkingstat() {
		return workingstat;
	}

	public void setWorkingstat(String workingstat) {
		this.workingstat = workingstat;
	}*/
}
