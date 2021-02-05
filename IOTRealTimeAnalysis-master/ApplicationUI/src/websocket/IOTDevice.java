package websocket;

import java.io.Serializable;
import java.util.Date;

public class IOTDevice implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int id;
	private double latitude;
	private double longitude;
	private int temp = 70;
	private Date timestamp = new Date();
	
	public IOTDevice(int id, double latitude, double longitude) {
		super();
		this.id = id;
		this.latitude = latitude;
		this.longitude = longitude;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}
	public double getLongitude() {
		return longitude;
	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}
	public int getTemp() {
		return temp;
	}
	public void setTemp(int temp) {
		this.temp = temp;
	}
	@Override
	public String toString() {
		return "IOTDevice [id=" + id + ", latitude=" + latitude + ", longitude=" + longitude + ", temp=" + temp + "]";
	}
	
	
}
