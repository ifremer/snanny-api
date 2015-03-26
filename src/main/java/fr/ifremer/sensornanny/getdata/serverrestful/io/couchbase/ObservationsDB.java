package fr.ifremer.sensornanny.getdata.serverrestful.io.couchbase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.SpatialViewRow;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

import fr.ifremer.sensornanny.getdata.serverrestful.exception.TooManyObservationsException;

public class ObservationsDB {

	private static final Logger logger = Logger.getLogger(ObservationsDB.class.getName());
	
	private static final double LATITUDE_BIN_SIZE  = Configuration.getInstance().syntheticLatitudeBinSize();
	private static final double LONGITUDE_BIN_SIZE = Configuration.getInstance().syntheticLongitudeBinSize();
	private static final long   TIME_BIN_SIZE      = Configuration.getInstance().syntheticTimeBinSize();
	private static final long   TIME_COEFF         = Configuration.getInstance().syntheticTimeBinCoeff();
	
	private static class Event {
		public double lat;
		public double lng;
		public long time;
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			long temp;
			temp = Double.doubleToLongBits(lat);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(lng);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			result = prime * result + (int) (time ^ (time >>> 32));
			return result;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Event other = (Event) obj;
			if (Double.doubleToLongBits(lat) != Double
					.doubleToLongBits(other.lat))
				return false;
			if (Double.doubleToLongBits(lng) != Double
					.doubleToLongBits(other.lng))
				return false;
			if (time != other.time)
				return false;
			return true;
		}
	}
	
	private static final Map<Event, Integer> CACHE = Collections.synchronizedMap(new HashMap<Event, Integer>());
	private static final AtomicBoolean CACHE_INITIALIZING = new AtomicBoolean();
	private static final AtomicBoolean CACHE_INITIALIZED  = new AtomicBoolean();
	
	public static void initialize() {
		try {
			if (!CACHE_INITIALIZED.get() && CACHE_INITIALIZING.compareAndSet(false, true)) {
				logger.info("Loading synthetic view in cache");
				CACHE_INITIALIZED.set(false);

				ViewQuery viewQuery = ViewQuery.from(Configuration.getInstance().observationsSyntheticViewDesign(), Configuration.getInstance().observationsSyntheticGeoTemporalCountViewName())
						.groupLevel(3)
						;

				ViewResult viewResponse = ConnectionManager.observations.query(viewQuery);

				for (ViewRow row : viewResponse.allRows()) {

					JsonArray key = (JsonArray) row.key();
					if (key != null) {
						Event event = new Event();
						event.lat  = Double.parseDouble(String.valueOf(key.get(0)));
						event.lng  = Double.parseDouble(String.valueOf(key.get(1)));
						event.time = Long.parseLong(String.valueOf(key.get(2)));
						CACHE.put(event, (int) row.value());
					}
				}

				CACHE_INITIALIZED.set(true);
				logger.info("Synthetic view successfully loaded in cache");
			}
			
			while (!CACHE_INITIALIZED.get()) {
				try {
					Thread.sleep(500);
				} catch (InterruptedException ignored) { }
			}
		} catch (Exception e) {
			logger.severe("Unable to retrieve all synthetic view, will be retryed next time...");
		} finally {
			CACHE_INITIALIZING.set(false);
		}
	}
	
    public JsonObject getObservations(String bboxQuery, String timeQuery) throws TooManyObservationsException {
    	JsonObject result = JsonObject.empty();
    	JsonArray features = JsonArray.empty();
    	
    	String[] bbox = bboxQuery.split(",");
    	
    	double queryLowerLatitude  = -90;
    	double queryLowerLongitude = -180;
    	double queryUpperLatitude  = 90;
    	double queryUpperLongitude = 180;
    	if (bbox.length == 4) {
    		queryLowerLatitude  = Double.parseDouble(bbox[0]);
    		queryLowerLongitude = Double.parseDouble(bbox[1]);
    		queryUpperLatitude  = Double.parseDouble(bbox[2]);
    		queryUpperLongitude = Double.parseDouble(bbox[3]);
    		
    		if (queryLowerLatitude  < -90)  queryLowerLatitude  = -90;
    		if (queryLowerLongitude < -180) queryLowerLongitude = -180;
    		if (queryUpperLatitude  > 90)   queryUpperLatitude  = 90;
    		if (queryUpperLongitude > 180)  queryUpperLongitude = 180;
    	}
    	
    	String[] time = timeQuery.split(",");
    	long queryBeginTime = 0l;
    	long queryEndTime   = System.currentTimeMillis();
    	if (time.length == 2) {
    		queryBeginTime = Long.parseLong(time[0]);
    		queryEndTime   = Long.parseLong(time[1]);
    	}
    	
    	try {
    		
    		double latDiff = ((queryUpperLatitude + 90.0) - (queryLowerLatitude + 90.0));
    		double lngDiff = ((queryUpperLongitude + 180.0) - (queryLowerLongitude + 180.0));
    		
    		if (
    				latDiff <= Configuration.getInstance().individualMaximumLatitudeRange() && lngDiff <= Configuration.getInstance().individualMaximumLongitudeRange()
    		) {
    			
    			SpatialViewQuery query = SpatialViewQuery.from(Configuration.getInstance().observationsIndividualViewDesign(), Configuration.getInstance().observationsIndividualGeoTemporalViewName())
    					.startRange(JsonArray.from(queryLowerLongitude, queryLowerLatitude, queryBeginTime))
    					.endRange(JsonArray.from(queryUpperLongitude, queryUpperLatitude, queryEndTime));
    			
    			long begin = System.currentTimeMillis();
    			SpatialViewResult spatialViewResult = ConnectionManager.observations.query(query, Configuration.getInstance().individualQueryTimeout(), TimeUnit.MILLISECONDS);
    			
    			List<SpatialViewRow> rows = spatialViewResult.allRows(Configuration.getInstance().individualCollectTimeout(), TimeUnit.MILLISECONDS);
    			
    			if (Configuration.getInstance().individualDebug()) {
    				logger.info("SPATIAL QUERY Took " + (System.currentTimeMillis() - begin) + " ms to find " + rows.size() + " matching document" + (rows.size() > 1 ? "s" : ""));
    			}
    			
    			if (rows.size() < 5000) {
    				for (SpatialViewRow row : rows) {
    					JsonObject feature = row.document().content();
    					
    					// add id to properties
    					feature.getObject("properties").put("id", row.id());
    					
    					features.add(feature);
    				}
    			}
    			
    			if (Configuration.getInstance().individualDebug()) {
    				logger.info("SPATIAL QUERY Documents Retrieving Took " + (System.currentTimeMillis() - begin) + " ms. Found " + features.size() + " measure" + (features.size() > 1 ? "s" : ""));
    			}
    		}
    		
    	} catch (RuntimeException runtimeException) {
    		try {
    			if (runtimeException.getCause() != null) {
    				throw runtimeException.getCause();
    			}
    		} catch (TimeoutException timeoutException) {
//    			logger.severe(timeoutException.toString());
//				throw new TooManyObservationsException(-1);
    		} catch (Throwable e) {
    			logger.severe(e.toString());
    		}
    	} catch (Exception e) {
    		logger.severe(e.toString());
    	}
    	
    	result.put("type", "FeatureCollection");
    	result.put("features", features);
    	
    	return result;
    }

	public JsonObject getObservationsCountForMapZoomUsingCache(String bboxQuery, String timeQuery) throws TooManyObservationsException {
		
		initialize();
		
		JsonObject result = JsonObject.empty();
		JsonArray features = JsonArray.empty();
		
		String[] bbox = bboxQuery.split(",");
		
		double queryLowerLatitude  = -90;
		double queryLowerLongitude = -180;
		double queryUpperLatitude  = 90;
		double queryUpperLongitude = 180;
		if (bbox.length == 4) {
			queryLowerLatitude  = Double.parseDouble(bbox[0]);
			queryLowerLongitude = Double.parseDouble(bbox[1]);
			queryUpperLatitude  = Double.parseDouble(bbox[2]);
			queryUpperLongitude = Double.parseDouble(bbox[3]);
			
			queryLowerLatitude  += 90;
			queryLowerLongitude += 180;
			queryUpperLatitude  += 90;
			queryUpperLongitude += 180;
			
			queryLowerLatitude  = Math.floor(queryLowerLatitude  * 10) / 10; 
			queryLowerLongitude = Math.floor(queryLowerLongitude * 10) / 10; 
			queryUpperLatitude  = Math.floor(queryUpperLatitude  * 10) / 10; 
			queryUpperLongitude = Math.floor(queryUpperLongitude * 10) / 10; 
			
			queryLowerLatitude  = queryLowerLatitude  - (queryLowerLatitude  % LATITUDE_BIN_SIZE);
			queryLowerLongitude = queryLowerLongitude - (queryLowerLongitude % LONGITUDE_BIN_SIZE);
			queryUpperLatitude  = queryUpperLatitude  - (queryUpperLatitude  % LATITUDE_BIN_SIZE);
			queryUpperLongitude = queryUpperLongitude - (queryUpperLongitude % LONGITUDE_BIN_SIZE);
			
			queryLowerLatitude  -= 90;
			queryLowerLongitude -= 180;
			queryUpperLatitude  -= 90;
			queryUpperLongitude -= 180;
			
			if (queryLowerLatitude  < -90)  queryLowerLatitude  = -90;
			if (queryLowerLongitude < -180) queryLowerLongitude = -180;
			if (queryUpperLatitude  > 90)  queryUpperLatitude  = 90;
			if (queryUpperLongitude > 180) queryUpperLongitude = 180;
		}
		
		String[] time = timeQuery.split(",");
		long queryBeginTime = 0l;
		long queryEndTime   = System.currentTimeMillis();
		if (time.length == 2) {
			queryBeginTime = Long.parseLong(time[0]);
			queryEndTime   = Long.parseLong(time[1]);

			queryBeginTime = queryBeginTime - (queryBeginTime % (TIME_BIN_SIZE * TIME_COEFF));
			if (queryBeginTime < 0) queryBeginTime = 0;

			queryEndTime = queryEndTime - (queryEndTime % (TIME_BIN_SIZE * TIME_COEFF)) + (TIME_BIN_SIZE * TIME_COEFF);
		}

		Map<String, Integer> countBySquare = new TreeMap<String, Integer>();

		long maximum = 0l;
		long maximumVisible = 0l;
		
		for (Map.Entry<Event, Integer> each : CACHE.entrySet()) {

			double latitude = each.getKey().lat;
			double longitude = each.getKey().lng;
			long timeBegin = each.getKey().time * TIME_COEFF;
			if (
					latitude >= queryLowerLatitude && latitude <= queryUpperLatitude
					&& longitude >= queryLowerLongitude && longitude <= queryUpperLongitude
					&& timeBegin >= queryBeginTime && timeBegin < queryEndTime
					) {
				String latLngAsString = String.valueOf(latitude) + ";" + String.valueOf(longitude);
				if (!countBySquare.containsKey(latLngAsString)) {
					countBySquare.put(latLngAsString, each.getValue());
				} else {
					Integer count = countBySquare.get(latLngAsString);
					countBySquare.put(latLngAsString, count + each.getValue());
				}
				if (each.getValue() > maximumVisible) {
					maximumVisible = each.getValue();
				}
			}
			if (timeBegin >= queryBeginTime && timeBegin < queryEndTime) {
				if (each.getValue() > maximum) {
					maximum = each.getValue();
				}
			}
		}

		for (Map.Entry<String, Integer> each : countBySquare.entrySet()) {
			String[] latLng = each.getKey().split(";");
			if (latLng.length == 2) {

				JsonObject feature = JsonObject.empty();

				feature.put("type", "Feature");

				JsonObject properties = JsonObject.empty();
				properties.put("count", each.getValue());
				properties.put("ratio", (each.getValue() * 100) / maximum);
				properties.put("ratio_visible", (each.getValue() * 100) / maximumVisible);
				feature.put("properties", properties);

				JsonObject geometry = JsonObject.empty();
				geometry.put("type", "Polygon");
				JsonArray coordinates = JsonArray.empty();
				JsonArray coordinates0 = JsonArray.empty();
				coordinates.add(coordinates0);

				double lowerLatitude  = Double.parseDouble(latLng[0]);
				double lowerLongitude = Double.parseDouble(latLng[1]);
				double upperLatitude  = lowerLatitude  + LATITUDE_BIN_SIZE;
				double upperLongitude = lowerLongitude + LONGITUDE_BIN_SIZE;

				coordinates0.add(JsonArray.empty().add(lowerLongitude).add(upperLatitude));
				coordinates0.add(JsonArray.empty().add(upperLongitude).add(upperLatitude));
				coordinates0.add(JsonArray.empty().add(upperLongitude).add(lowerLatitude));
				coordinates0.add(JsonArray.empty().add(lowerLongitude).add(lowerLatitude));
				coordinates0.add(JsonArray.empty().add(lowerLongitude).add(upperLatitude));

				geometry.put("coordinates", coordinates);
				feature.put("geometry", geometry);

				features.add(feature);
			}
		}

		result.put("type", "FeatureCollection");
		result.put("features", features);

		return result;
	}
	
	public JsonArray getObservationsCountForTimelineZoomUsingCache(String bboxQuery, String timeQuery) throws TooManyObservationsException {

		initialize();

		JsonArray result = JsonArray.empty();

		String[] bbox = bboxQuery.split(",");

		double queryLowerLatitude  = -90;
		double queryLowerLongitude = -180;
		double queryUpperLatitude  = 90;
		double queryUpperLongitude = 180;
		if (bbox.length == 4) {
			queryLowerLatitude  = Double.parseDouble(bbox[0]);
			queryLowerLongitude = Double.parseDouble(bbox[1]);
			queryUpperLatitude  = Double.parseDouble(bbox[2]);
			queryUpperLongitude = Double.parseDouble(bbox[3]);
			
			queryLowerLatitude  += 90;
			queryLowerLongitude += 180;
			queryUpperLatitude  += 90;
			queryUpperLongitude += 180;
			
			queryLowerLatitude  = Math.floor(queryLowerLatitude  * 10) / 10; 
			queryLowerLongitude = Math.floor(queryLowerLongitude * 10) / 10; 
			queryUpperLatitude  = Math.floor(queryUpperLatitude  * 10) / 10; 
			queryUpperLongitude = Math.floor(queryUpperLongitude * 10) / 10; 
			
			queryLowerLatitude  = queryLowerLatitude  - (queryLowerLatitude  % LATITUDE_BIN_SIZE);
			queryLowerLongitude = queryLowerLongitude - (queryLowerLongitude % LONGITUDE_BIN_SIZE);
			queryUpperLatitude  = queryUpperLatitude  - (queryUpperLatitude  % LATITUDE_BIN_SIZE);
			queryUpperLongitude = queryUpperLongitude - (queryUpperLongitude % LONGITUDE_BIN_SIZE);
			
			queryLowerLatitude  -= 90;
			queryLowerLongitude -= 180;
			queryUpperLatitude  -= 90;
			queryUpperLongitude -= 180;
			
			if (queryLowerLatitude  < -90)  queryLowerLatitude  = -90;
			if (queryLowerLongitude < -180) queryLowerLongitude = -180;
			if (queryUpperLatitude  > 90)  queryUpperLatitude  = 90;
			if (queryUpperLongitude > 180) queryUpperLongitude = 180;
		}
		
		String[] time = timeQuery.split(",");
		long queryBeginTime = 0l;
		long queryEndTime   = System.currentTimeMillis();
		if (time.length == 2) {
			queryBeginTime = Long.parseLong(time[0]);
			queryEndTime   = Long.parseLong(time[1]);

			queryBeginTime = queryBeginTime - (queryBeginTime % (TIME_BIN_SIZE * TIME_COEFF));
			if (queryBeginTime < 0) queryBeginTime = 0;

			queryEndTime = queryEndTime - (queryEndTime % (TIME_BIN_SIZE * TIME_COEFF)) + (TIME_BIN_SIZE * TIME_COEFF);
		}

		Map<Long, Integer> countByTimeRange = new TreeMap<Long, Integer>();

		for (Map.Entry<Event, Integer> each : CACHE.entrySet()) {

			double latitude = each.getKey().lat;
			double longitude = each.getKey().lng;
			long timeBegin = each.getKey().time * TIME_COEFF;
			if (
					latitude >= queryLowerLatitude && latitude <= queryUpperLatitude
					&& longitude >= queryLowerLongitude && longitude <= queryUpperLongitude
					&& timeBegin >= queryBeginTime && timeBegin < queryEndTime
					) {
				if (!countByTimeRange.containsKey(timeBegin)) {
					countByTimeRange.put(timeBegin, each.getValue());
				} else {
					Integer count = countByTimeRange.get(timeBegin);
					countByTimeRange.put(timeBegin, count + each.getValue());
				}
			}
		}

		for (Map.Entry<Long, Integer> each : countByTimeRange.entrySet()) {
			JsonObject event = JsonObject.empty();

			JsonObject eventTime = JsonObject.empty();
			long begin  = each.getKey();
			long end    = begin + (TIME_BIN_SIZE * TIME_COEFF);
			long center = (end + begin) / 2;
			event.put("event", center);

			eventTime.put("begin", begin);
			eventTime.put("end", end);
			event.put("time", eventTime);

			event.put("value", each.getValue());

			result.add(event);
		}

		return result;
	}

	public JsonObject getObservation(String id) {
		JsonDocument ret = ConnectionManager.observations.get(id);
		if (ret != null) {
			return ret.content();
		}
		return null;
	}
	
	public String getResultPath(String id) {
		String resultPath = null;
		JsonObject observation = getObservation(id);
		
		if (observation != null) {
			resultPath = observation.getObject("properties").getString("result");
		}
		
		return resultPath;
	}

}
