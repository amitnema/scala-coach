package org.apn.hadoop.mapreduce.maxtemp;

import java.sql.Date;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE = 9999;
	private String year;
	private int airTemperature;
	private String quality;
	private String stationId;
	private Date ObservationDate;

	public void parse(String record) {
		year = record.substring(15, 19);
		String airTemperatureString;
		// Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
		if (record.charAt(87) == '+') {
			airTemperatureString = record.substring(88, 92);
		} else {
			airTemperatureString = record.substring(87, 92);
		}
		airTemperature = Integer.parseInt(airTemperatureString);
		quality = record.substring(92, 93);
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	public boolean isValidTemperature() {
		return airTemperature != MISSING_TEMPERATURE && quality.matches("[01459]");
	}

	public String getYear() {
		return year;
	}

	public int getAirTemperature() {
		return airTemperature;
	}

	public String getStationId() {
		return stationId;
	}

	public void setStationId(String stationId) {
		this.stationId = stationId;
	}

	public Date getObservationDate() {
		return ObservationDate;
	}

	public void setObservationDate(Date observationDate) {
		ObservationDate = observationDate;
	}
}