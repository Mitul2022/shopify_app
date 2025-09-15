
CREATE TABLE IF NOT EXISTS dev.etl_watermarks (
    table_name TEXT PRIMARY KEY,
    last_watermark TIMESTAMPTZ
);

delete from dev.etl_watermarks;

INSERT INTO dev.etl_watermarks (table_name, last_watermark)
VALUES ('customers', '2000-01-01 00:00:00Z')
, ('orders', '2000-01-01 00:00:00Z')
, ('products', '2000-01-01 00:00:00Z')
, ('transactions', '2000-01-01 00:00:00Z')
, ('fulfillments', '2000-01-01 00:00:00Z')
ON CONFLICT (table_name) DO NOTHING;

select * from dev.etl_watermarks;

DROP TABLE IF EXISTS dev.customers;
CREATE TABLE IF NOT EXISTS dev.customers (
        customer_id BIGINT PRIMARY KEY,
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        first_name TEXT,
        last_name TEXT,
        orders_count BIGINT,
        state TEXT,
        total_spent NUMERIC(18,2),
        last_order_id BIGINT,
        note TEXT,
        verified_email BOOLEAN,
        multipass_identifier TEXT,
        tax_exempt BOOLEAN,
        tags TEXT,
        last_order_name TEXT,
        email TEXT,
        phone TEXT,
        currency TEXT,
        addresses JSONB,
        tax_exemptions JSONB,
        email_marketing_consent JSONB,
        sms_marketing_consent JSONB,
        admin_graphql_api_id TEXT,
        default_address TEXT
    );




-- DROP PROCEDURE dev.usp_insert_customer(jsonb);

CREATE OR REPLACE PROCEDURE dev.usp_insert_customer(IN _customers jsonb)
 LANGUAGE plpgsql
AS $procedure$
BEGIN

	TRUNCATE TABLE dev.customers;

    INSERT INTO dev.customers (
        customer_id, created_at, updated_at, first_name, last_name, orders_count, state,
        total_spent, last_order_id, note, verified_email, multipass_identifier, tax_exempt,
        tags, last_order_name, email, phone, currency, addresses,
        tax_exemptions, email_marketing_consent, sms_marketing_consent, admin_graphql_api_id, default_address
    )
    SELECT
        (cust->>'id')::NUMERIC::BIGINT,
        (cust->>'created_at')::TIMESTAMPTZ,
        (cust->>'updated_at')::TIMESTAMPTZ,
        cust->>'first_name',
        cust->>'last_name',
        (cust->>'orders_count')::BIGINT,
        cust->>'state',
        (cust->>'total_spent')::NUMERIC,
        NULLIF(cust->>'last_order_id','')::NUMERIC::BIGINT,
        NULLIF(cust->>'note',''),
        (cust->>'verified_email')::BOOLEAN,
        NULLIF(cust->>'multipass_identifier',''),
        (cust->>'tax_exempt')::BOOLEAN,
        NULLIF(cust->>'tags',''),
        NULLIF(cust->>'last_order_name',''),
        cust->>'email',
        NULLIF(cust->>'phone',''),
        cust->>'currency',
        cust->'addresses',
        cust->'tax_exemptions',
        cust->'email_marketing_consent',
        cust->'sms_marketing_consent',
        cust->>'admin_graphql_api_id',
		cust->>'default_address'
	FROM jsonb_array_elements(_customers) AS cust
	ON CONFLICT (customer_id) DO NOTHING;
END;
$procedure$
;


CREATE OR REPLACE PROCEDURE dev.usp_upsert_customer(IN _customers jsonb)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO dev.customers (
        customer_id, created_at, updated_at, first_name, last_name, orders_count, state,
        total_spent, last_order_id, note, verified_email, multipass_identifier, tax_exempt,
        tags, last_order_name, email, phone, currency, addresses,
        tax_exemptions, email_marketing_consent, sms_marketing_consent, admin_graphql_api_id, default_address
    )
    SELECT
        (cust->>'id')::NUMERIC::BIGINT,
        (cust->>'created_at')::TIMESTAMPTZ,
        (cust->>'updated_at')::TIMESTAMPTZ,
        cust->>'first_name',
        cust->>'last_name',
        (cust->>'orders_count')::BIGINT,
        cust->>'state',
        (cust->>'total_spent')::NUMERIC,
        NULLIF(cust->>'last_order_id','')::NUMERIC::BIGINT,
        NULLIF(cust->>'note',''),
        (cust->>'verified_email')::BOOLEAN,
        NULLIF(cust->>'multipass_identifier',''),
        (cust->>'tax_exempt')::BOOLEAN,
        NULLIF(cust->>'tags',''),
        NULLIF(cust->>'last_order_name',''),
        cust->>'email',
        NULLIF(cust->>'phone',''),
        cust->>'currency',
        cust->'addresses',
        cust->'tax_exemptions',
        cust->'email_marketing_consent',
        cust->'sms_marketing_consent',
        cust->>'admin_graphql_api_id',
		cust->>'default_address'
	FROM jsonb_array_elements(_customers) AS cust
	ON CONFLICT (customer_id)
    DO UPDATE SET
        first_name = EXCLUDED.first_name,
		last_name = EXCLUDED.last_name,
		addresses = EXCLUDED.addresses,
        email = EXCLUDED.email,
		phone = EXCLUDED.phone,
		orders_count = EXCLUDED.orders_count,
		total_spent = EXCLUDED.total_spent,
		updated_at = CURRENT_TIMESTAMP;
        
END;
$procedure$
;

--select CURRENT_TIMESTAMP

delete from dev.etl_watermarks;

INSERT INTO dev.etl_watermarks (table_name, last_watermark)
VALUES ('customers', '2000-01-01 00:00:00Z'), ('orders', '2000-01-01 00:00:00Z'), ('products', '2000-01-01 00:00:00Z')
ON CONFLICT (table_name) DO NOTHING;

select * from dev.etl_watermarks;

--=============================================

truncate table dev.customers;
select * from dev.customers order by updated_at desc








