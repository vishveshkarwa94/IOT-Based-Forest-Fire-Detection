package websocket;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

public class IOTDeviceMaster implements Serializable {
	 
	private ArrayList<IOTDevice> iotData;
	private Date date = new Date();

	public ArrayList<IOTDevice> getIotData() {
		return iotData;
	}

	public void setIotData(ArrayList<IOTDevice> iotData) {
		this.iotData = iotData;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}
	
	
}
