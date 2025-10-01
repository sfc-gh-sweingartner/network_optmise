-- Replace these values with your specific information
-- SET VARIABLES
SET APP_CREATOR_ROLE = 'ACCOUNTADMIN'; 
SET DB_NAME = 'TELCO_NETWORK_OPTIMIZATION_PROD';  
SET SCHEMA_NAME = 'RAW';    
SET APP_NAME = 'Network Optimisation';   
SET MAPBOX_API_KEY = '[paste your mapbox api key here]';      -- Your Mapbox API key


Use DATABASE  IDENTIFIER($DB_NAME);
Use SCHEMA  IDENTIFIER($SCHEMA_NAME);


-- Create network rule for map tile servers
CREATE OR REPLACE NETWORK RULE map_tile_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = (
    'api.mapbox.com', 
    'a.tiles.mapbox.com', 
    'b.tiles.mapbox.com', 
    'c.tiles.mapbox.com', 
    'd.tiles.mapbox.com'
  );

-- Create secret for Mapbox API key
CREATE OR REPLACE SECRET mapbox_key
  TYPE = GENERIC_STRING
  SECRET_STRING = $MAPBOX_API_KEY;

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION map_access_int
  ALLOWED_NETWORK_RULES = (map_tile_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (mapbox_key)
  ENABLED = TRUE;

-- Grant necessary privileges
GRANT READ ON SECRET mapbox_key TO ROLE IDENTIFIER($APP_CREATOR_ROLE);
GRANT USAGE ON INTEGRATION map_access_int TO ROLE IDENTIFIER($APP_CREATOR_ROLE);

--find the name of your streamlit app
  show streamlits;

-- Enable the Streamlit app to use the integration and secret
-- note you will need to change the streamlit app name to match the one you are using
ALTER STREAMLIT TELCO_NETWORK_OPTIMIZATION_PROD.RAW.FQYVZ89K8QDAWHRK
  SET EXTERNAL_ACCESS_INTEGRATIONS = (map_access_int)
  SECRETS = ('mapbox_key' = TELCO_NETWORK_OPTIMIZATION_PROD.RAW.mapbox_key);

  
-- Verify configuration
SHOW NETWORK RULES LIKE 'map_tile_rule';
SHOW SECRETS LIKE 'mapbox_key';
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'map_access_int'; 

 SHOW SECRETS LIKE 'mapbox_key' IN TELCO_NETWORK_OPTIMIZATION_PROD.RAW;

-- note you will need to change the streamlit app name to match the one you are using
 ALTER STREAMLIT TELCO_NETWORK_OPTIMIZATION_PROD.RAW.FQYVZ89K8QDAWHRK
  SET EXTERNAL_ACCESS_INTEGRATIONS = (map_access_int)
  SECRETS = ('mapbox_key' = mapbox_key);

 