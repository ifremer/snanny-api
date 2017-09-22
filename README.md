# snanny-api
restful api to read observations in sensor nanny repostory

## Build and deploy
tested with java 1.8 and maven 3.0.4 or above

git clone https://github.com/ifremer/snanny-api.git

mvn install -DskipTests

deploy war in tomcat

## Configuration
Configuration files are :
  - application.properties
    Configure elasticsearch clusters and search properties
    
  	
## Endpoints

## ElasticSearch based api

GET /api/rest/obs/synthetic/timeline?bbox=lat1,lon2,lat2,lon2&kwords=keyword

	Get synthetic view (time ranges with count) matching criteria
	Configuration in synthetic.properties

	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	keyword : search keyword, each sequences separated by a comma
	

GET /api/rest/obs/synthetic/map?bbox=lat1,lon2,lat2,lon2&time=begin,end&kwords=keyword

	Get synthetic view (geo boxes with count) matching criteria
	Configuration in synthetic.properties

	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	begin,end : timestamp in ms
	keyword : search keyword, each sequences separated by a comma
	
	
GET /api/rest/obs?bbox=lat1,lon2,lat2,lon2&time=begin,end&kwords=keyword

	Get individual points of observation matching criteria
	Configuration in individual.properties

	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	begin,end : timestamp in ms
	keyword : search keyword, each sequences separated by a comma
	
GET /api/rest/obs/scroll?id=scrollId

	Get individual points of observation matching criteria
	Configuration in individual.properties

	scrollId : pagination identifier to retrieve next observations of a previous query
	
GET /api/rest/systems?bbox=lat1,lon2,lat2,lon2&time=begin,end&kwords=keyword

	Get all the systems informations 
	
	lat1,lat2 : range from -90 to 90
	lon1,lon2 : range from -180 to 180
	begin,end : timestamp in ms
	keyword : search keyword, each sequences separated by a comma
	
GET /api/rest/system/{uuid}
    
    Retrieve all the system informations from its uuid
    This method return result when a uuid is part of the deployed system 
    For example a system with uuid can have many deployment 
    
GET /api/rest/system/deployement/{uuid}\_{startdate}\_{enddate}
    
    Retrieve a specific system information from its uuid and deployment date
    This method return the specific deployment of a system 