-- Mapbox Integration Setup Script for Cortex Analyst V3 (Revised for Default Map Access)
-- This script configures network access for Mapbox tiles in Snowflake for the Streamlit app,
-- relying on Streamlit's default map rendering capabilities.
-- Prerequisites: ACCOUNTADMIN role access.

---------------------------------------------------------------------------
-- STEP 1: Set Configuration Variables
---------------------------------------------------------------------------
SET APP_CREATOR_ROLE = 'ACCOUNTADMIN';
SET DB_NAME = 'CortexChartsV4'; -- Replace if your DB name is different
SET SCHEMA_NAME = 'CortexChartsV4'; -- Replace if your Schema name is different
-- SET MAPBOX_API_KEY = 'YOUR_MAPBOX_KEY'; -- This line is REMOVED

---------------------------------------------------------------------------
-- STEP 2: Create Network Rule for Mapbox Servers
---------------------------------------------------------------------------
-- This rule allows egress to Mapbox tile servers, which st.map() uses by default.
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

-- Verify network rule creation
SHOW NETWORK RULES LIKE 'map_tile_rule';

---------------------------------------------------------------------------
-- STEP 3: Create Secret for Mapbox API Key (REMOVED)
---------------------------------------------------------------------------
-- This step is removed as we are trying to rely on default map access
-- without a user-provided API key.
-- CREATE OR REPLACE SECRET mapbox_key
-- TYPE = GENERIC_STRING
-- SECRET_STRING = $MAPBOX_API_KEY;
-- SHOW SECRETS LIKE 'mapbox_key';

---------------------------------------------------------------------------
-- STEP 4: Create External Access Integration
---------------------------------------------------------------------------
-- The integration now only references the network rule, not an API key secret.
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION map_access_int
  ALLOWED_NETWORK_RULES = (map_tile_rule)
  -- ALLOWED_AUTHENTICATION_SECRETS = (mapbox_key) -- This line is REMOVED
  ENABLED = TRUE;

-- Verify integration creation
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'map_access_int';

---------------------------------------------------------------------------
-- STEP 5: Grant Necessary Privileges
---------------------------------------------------------------------------
-- Grant access to the secret (REMOVED)
-- GRANT READ ON SECRET mapbox_key
-- TO ROLE IDENTIFIER($APP_CREATOR_ROLE);

-- Grant access to the integration
GRANT USAGE ON INTEGRATION map_access_int
  TO ROLE IDENTIFIER($APP_CREATOR_ROLE);

---------------------------------------------------------------------------
-- STEP 6: Configure Streamlit App Access
---------------------------------------------------------------------------
-- Run SHOW STREAMLITS; first to find your app's auto-generated name if you are unsure.
-- The name is often in the format <DB_NAME>.<SCHEMA_NAME>.<APP_INSTANCE_NAME_FROM_YAML_OR_AUTO_GENERATED>
-- For example, if your YAML defined `application:` `name: CortexApp`, it might be CortexChartsV4.CortexChartsV4.CortexApp
-- If it was auto-generated, it will be a longer unique ID.
SHOW STREAMLITS;

-- Replace 'REPLACE_WITH_YOUR_APP_NAME' with your actual Streamlit app's name or auto-generated ID.
-- Ensure the name is fully qualified if it's not being run in the context of the correct DB/Schema.
-- Example: ALTER STREAMLIT CortexChartsV4.CortexChartsV4."MY_COOL_STREAMLIT_APP"
-- The SECRETS clause is removed.
ALTER STREAMLIT IDENTIFIER('"REPLACE_WITH_YOUR_STREAMLIT_APP_NAME_PATH"')
  SET EXTERNAL_ACCESS_INTEGRATIONS = (map_access_int);
  -- SECRETS = ('mapbox_key' = mapbox_key); -- This line is REMOVED

-- Note on Streamlit app name:
-- If your Streamlit app object was named "CortexApp" and resides in DB_NAME.SCHEMA_NAME,
-- the identifier might be 'CortexChartsV4.CortexChartsV4.CortexApp'.
-- Use SHOW STREAMLITS; to confirm the exact name and copy it.
-- If the name contains spaces or special characters, or is case-sensitive and not all uppercase,
-- ensure it's enclosed in double quotes. The IDENTIFIER keyword can be helpful.

---------------------------------------------------------------------------
-- STEP 7: Verify Final Configuration
---------------------------------------------------------------------------
-- Run these commands to verify all components are properly configured
SHOW NETWORK RULES LIKE 'map_tile_rule';
-- SHOW SECRETS LIKE 'mapbox_key'; -- Secret is no longer used
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE 'map_access_int';
SHOW STREAMLITS; -- Check your app's configuration

---------------------------------------------------------------------------
-- Troubleshooting Guide
---------------------------------------------------------------------------
-- If you encounter issues:
-- 1. Verify all SHOW commands return expected results for the objects that are still created.
-- 2. Ensure your app name is correct in the ALTER STREAMLIT statement (use the fully qualified name from SHOW STREAMLITS).
-- 3. Confirm your role has ACCOUNTADMIN privileges (or sufficient privileges to create/alter these objects and grant usage).
-- 4. Verify the database and schema names match your deployment.
-- 5. Try refreshing your Streamlit app after configuration.
-- 6. If maps still don't appear, it might be that Streamlit's default token is not functioning as expected in the Snowflake environment,
--    or has rate limits that are being hit. In such a scenario, using a dedicated Mapbox API key (as per your original script)
--    would be the more reliable solution, despite the desire to avoid it.