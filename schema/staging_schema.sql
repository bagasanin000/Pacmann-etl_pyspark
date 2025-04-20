CREATE TABLE company (
    object_id SERIAL PRIMARY KEY,
    description TEXT,
    region VARCHAR(255),
    address1 VARCHAR(255),
    address2 VARCHAR(255),
    city VARCHAR(255),
    zip_code VARCHAR(20),
    state_code VARCHAR(20),
    country_code VARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE acquisition (
    acquisition_id SERIAL PRIMARY KEY,
    acquiring_object_id INT REFERENCES company(object_id) ON DELETE CASCADE,
    acquired_object_id INT REFERENCES company(object_id) ON DELETE CASCADE,
    term_code VARCHAR(255),
    price_amount NUMERIC,
    price_currency_code VARCHAR(50),
    acquired_at DATE,
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);


CREATE TABLE ipo (
    ipo_id SERIAL PRIMARY KEY,
    object_id INT REFERENCES company(object_id),
    valuation_amount FLOAT,
    valuation_currency_code VARCHAR(10),
    raised_amount FLOAT,
    raised_currency_code VARCHAR(10),
    public_at TIMESTAMP,
    stock_symbol VARCHAR(50),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE funding_rounds (
    funding_round_id SERIAL PRIMARY KEY,
    object_id INT REFERENCES company(object_id),
    funded_at TIMESTAMP,
    funding_round_type VARCHAR(255),
    funding_round_code VARCHAR(255),
    raised_amount_usd FLOAT,
    raised_amount FLOAT,
    raised_currency_code VARCHAR(10),
    pre_money_valuation_usd FLOAT,
    pre_money_valuation FLOAT,
    pre_money_currency_code VARCHAR(10),
    post_money_valuation_usd FLOAT,
    post_money_valuation FLOAT,
    post_money_currency_code VARCHAR(10),
    participants INT,
    is_first_round BOOLEAN,
    is_last_round BOOLEAN,
    source_url TEXT,
    source_description TEXT,
    created_by VARCHAR(255),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE investments (
    investment_id SERIAL PRIMARY KEY,
    funding_round_id INT REFERENCES funding_rounds(funding_round_id),
    funded_object_id INT REFERENCES company(object_id),
    investor_object_id INT REFERENCES company(object_id),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE funds (
    fund_id SERIAL PRIMARY KEY,
    object_id INT REFERENCES company(object_id),
    name VARCHAR(255),
    funded_at TIMESTAMP,
    raised_amount FLOAT,
    raised_currency_code VARCHAR(10),
    source_url TEXT,
    source_description TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE milestones (
    created_at TIMESTAMP,
    description TEXT,
    milestone_at DATE,
    milestone_code VARCHAR(255),
    milestone_id SERIAL PRIMARY KEY,
    object_id INT REFERENCES company(object_id) ON DELETE CASCADE,
    source_description TEXT,
    source_url TEXT,
    updated_at TIMESTAMP
);

-- Create sequence for people table
CREATE SEQUENCE people_people_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Create sequence for relationship table
CREATE SEQUENCE relationship_relationship_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;



CREATE TABLE people (
    people_id BIGINT DEFAULT nextval('people_people_id_seq') PRIMARY KEY,
    object_id INTEGER,                       
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    birthplace VARCHAR(255),
    affiliation_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_people_company
        FOREIGN KEY (object_id) REFERENCES company(object_id)
);

CREATE TABLE relationship (
    relationship_id BIGINT DEFAULT nextval('relationship_relationship_id_seq') PRIMARY KEY,
    person_object_id INTEGER,
    relationship_object_id INTEGER,
    start_at DATE,
    end_at DATE,
    is_past BOOLEAN,
    sequence INTEGER,
    title VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_relationship_person
        FOREIGN KEY (person_object_id) REFERENCES people(people_id) 
        ON DELETE CASCADE,

    CONSTRAINT fk_relationship_company
        FOREIGN KEY (relationship_object_id) REFERENCES company(object_id)
);

-- Fixing loading error
-- IPO
ALTER TABLE ipo 
DROP CONSTRAINT ipo_object_id_fkey,
ADD CONSTRAINT ipo_object_id_fkey
    FOREIGN KEY (object_id) REFERENCES company(object_id) 
    ON DELETE CASCADE;

-- Funding Rounds
ALTER TABLE funding_rounds 
DROP CONSTRAINT funding_rounds_object_id_fkey,
ADD CONSTRAINT funding_rounds_object_id_fkey
    FOREIGN KEY (object_id) REFERENCES company(object_id) 
    ON DELETE CASCADE;

-- Investments (funded_object_id)
ALTER TABLE investments 
DROP CONSTRAINT investments_funded_object_id_fkey,
ADD CONSTRAINT investments_funded_object_id_fkey
    FOREIGN KEY (funded_object_id) REFERENCES company(object_id) 
    ON DELETE CASCADE;

-- Investments (investor_object_id)
ALTER TABLE investments 
DROP CONSTRAINT investments_investor_object_id_fkey,
ADD CONSTRAINT investments_investor_object_id_fkey
    FOREIGN KEY (investor_object_id) REFERENCES company(object_id) 
    ON DELETE CASCADE;

-- Funds
ALTER TABLE funds 
DROP CONSTRAINT funds_object_id_fkey,
ADD CONSTRAINT funds_object_id_fkey
    FOREIGN KEY (object_id) REFERENCES company(object_id) 
    ON DELETE CASCADE;
