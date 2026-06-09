-- InvestSphere governance, step 2: role-based access control (RBAC).
-- Grants are given to GROUPS (create these groups in your account first), never
-- to individual users. This is the "who can see what" layer.
--
-- Groups used:
--   investsphere_engineers - build pipelines, full read/write
--   investsphere_analysts  - read Gold analytics only
--   investsphere_pms       - portfolio managers, read Gold but row-filtered (step 3)

-- Everyone who uses the platform needs USE on the catalog.
GRANT USE CATALOG ON CATALOG investsphere TO `investsphere_engineers`;
GRANT USE CATALOG ON CATALOG investsphere TO `investsphere_analysts`;
GRANT USE CATALOG ON CATALOG investsphere TO `investsphere_pms`;

-- Engineers: full access to all layers.
GRANT ALL PRIVILEGES ON SCHEMA investsphere.bronze TO `investsphere_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA investsphere.silver TO `investsphere_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA investsphere.gold   TO `investsphere_engineers`;

-- Analysts and PMs: read Gold only (not raw Bronze / Silver).
GRANT USE SCHEMA ON SCHEMA investsphere.gold TO `investsphere_analysts`;
GRANT SELECT     ON SCHEMA investsphere.gold TO `investsphere_analysts`;
GRANT USE SCHEMA ON SCHEMA investsphere.gold TO `investsphere_pms`;
GRANT SELECT     ON SCHEMA investsphere.gold TO `investsphere_pms`;
