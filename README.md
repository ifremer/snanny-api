# snanny-api
restful api to read observations in sensor nanny repostory

## Build and deploy
tested with java 1.8 and maven 3.0.4 or above

git clone https://github.com/ifremer/snanny-api.git

mvn install

deploy war in tomcat

## Configuration
Configuration files are :
  - couchbase.properties
  	Configure couchbase cluster and views used by API
  - synthetic.properties
  	Configuration for synthetic view
  - individual.properties
  	Configuration for individual view
  	
## Endpoints

GET /api/rest/observations/synthetic/timeline?bbox=lat1,lon2,lat2,lon2&time=begin,end

	Get synthetic view (time ranges with count) matching criteria
	Configuration in synthetic.properties

	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	begin,end : timestamp in ms
	
	
GET /api/rest/observations/synthetic/map?bbox=lat1,lon2,lat2,lon2&time=begin,end

	Get synthetic view (geo boxes with count) matching criteria
	Configuration in synthetic.properties

	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	begin,end : timestamp in ms
	
	
GET /api/rest/observations?bbox=lat1,lon2,lat2,lon2&time=begin,end

	Get individual points of observation matching criteria
	Configuration in individual.properties

	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	begin,end : timestamp in ms 