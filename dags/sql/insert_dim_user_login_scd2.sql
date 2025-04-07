WITH source_data AS (
    SELECT
        tenant_id,
        user_id,
        login_type,
        login_value,
        active,
        CASE
            WHEN active::BOOLEAN IS TRUE THEN active_to::DATE
            ELSE active_from::DATE
            END AS active_from,
        CASE
            WHEN active::BOOLEAN IS FALSE THEN active_to::DATE
            ELSE '9999-01-01'::DATE
            END AS active_to
    FROM dwh_dict.dim_user_login_scd2_staging
)
INSERT INTO dwh_dict.dim_user_login_scd2 (
    tenant_id,
    user_id,
    login_type,
    login_value,
    active,
    active_from,
    active_to
)
SELECT * FROM source_data
WHERE active_from <> active_to
ON CONFLICT (active_from, tenant_id, login_value, login_type)
DO UPDATE SET
    user_id = EXCLUDED.user_id,
    active = EXCLUDED.active,
    active_to = EXCLUDED.active_to;