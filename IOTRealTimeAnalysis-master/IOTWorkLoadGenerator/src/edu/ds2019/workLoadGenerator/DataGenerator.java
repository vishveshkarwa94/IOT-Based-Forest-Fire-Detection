package edu.ds2019.workLoadGenerator;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataGenerator {

	private static int nextId = 0;
	private static double latitude = 47;
	private static double longitude = -122;
	private static HashMap<ArrayList<Integer>, IOTDevice> deviceMap = new HashMap<ArrayList<Integer>, IOTDevice>();
	private static HashMap<IOTDevice, HashMap<String, IOTDevice>> gridMap = new HashMap<IOTDevice, HashMap<String, IOTDevice>>();

	private static HashMap<Integer, IOTDevice> modifiedMapIOT = new HashMap<>();

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<String>> grid = gridGenerate(44, 88);
		generateMap(grid);
		runKafkaForMap();
		traverseFire(20, 0, "E");
		System.out.println("END");
	}

	/*private static void startAllNodes() {
		for(IOTDevice device : gridMap.keySet()) {
			ForestFireProducer produceFire = new ForestFireProducer(device);
			new Thread(produceFire).start();
		}
	}*/

	private static void runKafkaForMap(){
		ObjectMapper mapper=new ObjectMapper();
		System.out.println("count:"+modifiedMapIOT.values().size());
		for(IOTDevice device: modifiedMapIOT.values()){
			try {
				ForestFireProducer.kafkaProducer(mapper.writeValueAsString(device), device.getId());
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	private static void traverseFire(int row, int column, String directn) {
		ArrayList<Integer> startCoord = new ArrayList<Integer>(Arrays.asList(row, column));
		if( !deviceMap.containsKey(startCoord)) {
			
			if(deviceMap.containsKey(Arrays.asList(startCoord.get(0)+1, startCoord.get(1)))) {
				startCoord.set(0, startCoord.get(0)+1);
			}
			if(deviceMap.containsKey(Arrays.asList(startCoord.get(0)-1, startCoord.get(1)))) {
				startCoord.set(0, startCoord.get(0)-1);
			}
			if(deviceMap.containsKey(Arrays.asList(startCoord.get(0), startCoord.get(1)+1))) {
				startCoord.set(1, startCoord.get(1)+1);
			}
			if(deviceMap.containsKey(Arrays.asList(startCoord.get(0), startCoord.get(1)-1))) {
				startCoord.set(1, startCoord.get(1)-1);
			}
		}
		Random rand = new Random();
		IOTDevice startDevice = deviceMap.get(new ArrayList<Integer>(Arrays.asList(startCoord.get(0), startCoord.get(1))));
		startDevice.setTemp(rand.ints(80, 100).findFirst().getAsInt());
		/*
		 * ForestFireProducer produceFire = new ForestFireProducer(startDevice); new
		 * Thread(produceFire).start(); try { Thread.sleep(5000); } catch
		 * (InterruptedException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
		HashMap<Integer, Boolean> visited = new HashMap<Integer, Boolean>();
		visited.put(startDevice.getId(), true);
		ArrayList<IOTDevice> queue = new ArrayList<IOTDevice>();
		queue.add(startDevice);
		while(queue.size() > 0) {
			IOTDevice device = queue.remove(0);
			//System.out.println("dev"+device);
			IOTDevice device1 = null;
			IOTDevice device2 = null;
			IOTDevice device3 = null;
			if(gridMap.containsKey(device) && gridMap.get(device).containsKey(directn)) {
				if(directn.equals("N")) {
					device1 = gridMap.get(device).get("N");
					if(gridMap.get(device).containsKey("NE")) {
						device2 = gridMap.get(device).get("NE");
					}
					if(gridMap.get(device).containsKey("NW")) {
						device3 = gridMap.get(device).get("NW");
					}
				}
				if(directn.equals("NE")) {
					device1 = gridMap.get(device).get("NE");
					if(gridMap.get(device).containsKey("N")) {
						device2 = gridMap.get(device).get("N");
					}
					if(gridMap.get(device).containsKey("E")) {
						device3 = gridMap.get(device).get("E");
					}
				}
				if(directn.equals("E")) {
					device1 = gridMap.get(device).get("E");
					if(gridMap.get(device).containsKey("NE")) {
						device2 = gridMap.get(device).get("NE");
					}
					if(gridMap.get(device).containsKey("SE")) {
						device3 = gridMap.get(device).get("SE");
					}
				}
				if(directn.equals("SE")) {
					device1 = gridMap.get(device).get("SE");
					if(gridMap.get(device).containsKey("E")) {
						device2 = gridMap.get(device).get("E");
					}
					if(gridMap.get(device).containsKey("S")) {
						device3 = gridMap.get(device).get("S");
					}
				}
				if(directn.equals("S")) {
					device1 = gridMap.get(device).get("S");
					if(gridMap.get(device).containsKey("SE")) {
						device2 = gridMap.get(device).get("SE");
					}
					if(gridMap.get(device).containsKey("SW")) {
						device3 = gridMap.get(device).get("SW");
					}
				}
				if(directn.equals("SW")) {
					device1 = gridMap.get(device).get("SW");
					if(gridMap.get(device).containsKey("S")) {
						device2 = gridMap.get(device).get("S");
					}
					if(gridMap.get(device).containsKey("W")) {
						device3 = gridMap.get(device).get("W");
					}
				}
				if(directn.equals("W")) {
					device1 = gridMap.get(device).get("W");
					if(gridMap.get(device).containsKey("SW")) {
						device2 = gridMap.get(device).get("SW");
					}
					if(gridMap.get(device).containsKey("NW")) {
						device3 = gridMap.get(device).get("NW");
					}
				}
				if(directn.equals("NW")) {
					device1 = gridMap.get(device).get("NW");
					if(gridMap.get(device).containsKey("W")) {
						device2 = gridMap.get(device).get("W");
					}
					if(gridMap.get(device).containsKey("N")) {
						device3 = gridMap.get(device).get("N");
					}
				}
				
				/*
				 * System.out.println(device1); System.out.println(device2);
				 * System.out.println(device3);
				 */
				modifiedMapIOT = new HashMap<>();
				if(device1 != null && !visited.containsKey(device1.getId())) {
					device1.setTemp(rand.ints(80, 100).findFirst().getAsInt());
					modifiedMapIOT.put(device1.getId(), device1);
					visited.put(device1.getId(), true);
					queue.add(device1);
					/*
					 * produceFire = new ForestFireProducer(device1); new
					 * Thread(produceFire).start();
					 */
				}
				if(device2 != null && !visited.containsKey(device2.getId())) {
					device2.setTemp(rand.ints(80, 100).findFirst().getAsInt());
					modifiedMapIOT.put(device2.getId(), device2);
					visited.put(device2.getId(), true);
					queue.add(device2);
					/*
					 * produceFire = new ForestFireProducer(device2); new
					 * Thread(produceFire).start();
					 */
				}
				if(device3 != null && !visited.containsKey(device3.getId())) {
					device3.setTemp(rand.ints(80, 100).findFirst().getAsInt());
					modifiedMapIOT.put(device3.getId(), device3);
					visited.put(device3.getId(), true);
					queue.add(device3);
					/*
					 * produceFire = new ForestFireProducer(device3); new
					 * Thread(produceFire).start();
					 */
				}
				
				try {
//					dumpDataCitiesJSON();
					runKafkaForMap();
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	private static ArrayList<ArrayList<String>> gridGenerate(int rows, int columns){
		ArrayList<ArrayList<String>> grid = new ArrayList<ArrayList<String>>();
		String data = "0";
		for(int row=0; row < rows; row++) {
			ArrayList<String> subGrid = new ArrayList<String>();
			for(int col=0; col < columns; col++) {
				subGrid.add(data);
				if(data.equalsIgnoreCase("0")) {
					data = "1";
				}
				else {
					data = "0";
				}
			}
			grid.add(subGrid);
			if(data.equalsIgnoreCase("0")) {
				data = "1";
			}
			else {
				data = "0";
			}
		}
		return grid;
		
	}

	private static void generateMap(ArrayList<ArrayList<String>> grid){
		
		for(int row=0; row < grid.size(); row++) {
			for(int col=0; col < grid.get(row).size(); col++) {
				if(grid.get(row).get(col).equals("1")) {
					IOTDevice device = new IOTDevice(nextId, latitude, longitude);
					deviceMap.put(new ArrayList<Integer>(Arrays.asList(row, col)), device);
					modifiedMapIOT.put(device.getId(), device);
					nextId += 1;
					longitude += 0.3;
				}
			}
			latitude -= 0.3;
			longitude = -122;
		}
		for(int row=0; row < grid.size(); row++) {
			for(int col=0; col < grid.get(row).size(); col++) {
				if(grid.get(row).get(col).equals("1")) {
					gridMap.put(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col))), new HashMap<String, IOTDevice>());
					if((row - 2) > 0) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("N", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row-2, col))));
					}
					if((row - 1) > 0 && (col + 1) < grid.get(row).size()) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("NE", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row-1, col+1))));
					}
					if((col + 2) < grid.get(row).size()) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("E", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col+2))));
					}
					if((col+1) < grid.get(row).size() && (row+1)<grid.size()) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("SE", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row+1, col+1))));
					}
					if((row + 2) < grid.size()) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("S", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row+2, col))));
					}
					if((row + 1) < grid.size() && (col-1) > 0) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("SW", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row+1, col-1))));
					}
					if((col - 2) > 0) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("W", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col-2))));
					}
					if((row - 1) > 0 && (col-1)>0) {
						gridMap.get(deviceMap.get(new ArrayList<Integer>(Arrays.asList(row, col)))).put("NW", deviceMap.get(new ArrayList<Integer>(Arrays.asList(row-1, col-1))));
					}
				}
			}
		}
	}
}
