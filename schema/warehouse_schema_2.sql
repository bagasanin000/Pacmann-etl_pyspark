-- ACTIVATE POSTGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- dim_company
CREATE TABLE IF NOT EXISTS dim_company (
    company_id SERIAL PRIMARY KEY,
    company_entity_type VARCHAR(5) NOT NULL,
    company_object_id INT NOT NULL,
    description VARCHAR(255),
    address VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255),
    zip_code VARCHAR(255),
    state_code VARCHAR(255),
    country_code VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_company_object_id 
ON dim_company (company_object_id);

-- dim_people
CREATE TABLE IF NOT EXISTS dim_people (
    people_id SERIAL PRIMARY KEY,
    people_entity_type VARCHAR(5) NOT NULL,
    people_object_id INT NOT NULL,
    full_name VARCHAR(255),
    birthplace VARCHAR(255),
    affiliation_name VARCHAR(255),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_people_object_id 
ON dim_people (people_object_id);

-- dim_milestones
CREATE TABLE IF NOT EXISTS dim_milestones (
    milestone_id SERIAL PRIMARY KEY,
    milestone_entity_type VARCHAR(5) NOT NULL,
    milestone_object_id INT NOT NULL,
    milestone_date DATE,
    description TEXT,
    source_url VARCHAR(255),
    source_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key 
    CONSTRAINT fk_milestone_object
        FOREIGN KEY (milestone_object_id)
        REFERENCES dim_company(company_object_id)
        ON DELETE SET NULL
);

-- fact_acquisition
CREATE TABLE IF NOT EXISTS fact_acquisition (
    acquisition_id SERIAL PRIMARY KEY,
    acquiring_entity_type VARCHAR(10) NOT NULL,
    acquiring_object_id INT NOT NULL,
    acquired_entity_type VARCHAR(10) NOT NULL,
    acquired_object_id INT NOT NULL,
    price_amount NUMERIC(15, 2),
    price_currency_code VARCHAR(10),
    acquired_at DATE,
    source_url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Composite Foreign Key Constraints
    CONSTRAINT fk_acquiring_company FOREIGN KEY (acquiring_object_id)
        REFERENCES dim_company(company_object_id)
		ON DELETE SET NULL,
    CONSTRAINT fk_acquired_company FOREIGN KEY (acquired_object_id)
        REFERENCES dim_company(company_object_id)
		ON DELETE SET NULL
);


-- dim_funding_rounds
CREATE TABLE IF NOT EXISTS dim_funding_rounds (
    funding_round_id INT PRIMARY KEY,
    funding_entity_type VARCHAR(10), 
    funding_object_id INT, 
    round_type VARCHAR(100),
    funding_date DATE,
    raised_currency VARCHAR(10), 
    raised_amount DECIMAL(15, 2),
    raised_amount_usd DECIMAL(15, 2),
    pre_money_currency VARCHAR(10), 
    pre_money_valuation DECIMAL(15, 2),
    pre_money_valuation_usd DECIMAL(15, 2),
    post_money_currency VARCHAR(10), 
    post_money_valuation DECIMAL(15, 2),
    post_money_valuation_usd DECIMAL(15, 2),
    participants INT,
    source_url VARCHAR(255),
    source_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	
    -- FK dropped, validation via ETL
);



-- fact_investments
CREATE TABLE IF NOT EXISTS fact_investments (
    investment_id INT PRIMARY KEY,
    funding_round_id INT NOT NULL,
    funded_entity_type VARCHAR(10) NOT NULL,
    funded_object_id INT NOT NULL,
    investor_entity_type VARCHAR(10) NOT NULL,
    investor_object_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Hanya FK ke funding_rounds (bukan ke company/people)
    CONSTRAINT fk_funding_round FOREIGN KEY (funding_round_id) REFERENCES dim_funding_rounds(funding_round_id)
);



-- table fact_ipo
CREATE TABLE IF NOT EXISTS fact_ipo (
    ipo_id INT PRIMARY KEY,
	ipo_entity_type VARCHAR(10),
    ipo_object_id INT,
    valuation_currency VARCHAR(10),
    valuation_amount NUMERIC(15, 2),
    raised_currency VARCHAR(10),
    raised_amount NUMERIC(15, 2),
    public_at DATE,
	stock_market VARCHAR(20),
    stock_symbol VARCHAR(20),
	source_url VARCHAR(255),
	source_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign Key Constraints
	CONSTRAINT fk_ipo_company FOREIGN KEY (ipo_object_id) REFERENCES dim_company(company_object_id) DEFERRABLE INITIALLY DEFERRED
);


-- dim_funds
CREATE TABLE IF NOT EXISTS dim_funds (
    fund_id INT PRIMARY KEY,
    fund_entity_type VARCHAR(10), 
    fund_object_id INT, 
    fund_name VARCHAR(255),
    funding_date DATE,
    raised_currency VARCHAR(10), 
    raised_amount DECIMAL(15, 2),
    source_url VARCHAR(255),
    source_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
