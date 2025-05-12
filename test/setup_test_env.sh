#!/bin/bash
set -e

# PostgreSQL settings (ARM64 compatible)
PG_PASSWORD="YourStrongPassw0rd"
DATABASE_NAME="SimpleAdventureWorks"
CONTAINER_NAME="datahub_test_postgres"
NETWORK_NAME="datahub_network"

# Check if datahub_network exists
if ! docker network ls | grep -q $NETWORK_NAME; then
  echo "=== Creating DataHub network ==="
  docker network create $NETWORK_NAME
else
  echo "=== DataHub network already exists ==="
fi

# Remove existing container if it exists
if docker ps -a | grep -q $CONTAINER_NAME; then
  echo "=== Removing existing PostgreSQL container ==="
  docker stop $CONTAINER_NAME 2>/dev/null || true
  docker rm $CONTAINER_NAME 2>/dev/null || true
fi

echo "=== Starting PostgreSQL container (ARM64 compatible) ==="
docker run --name $CONTAINER_NAME \
  --network $NETWORK_NAME \
  -e POSTGRES_PASSWORD=$PG_PASSWORD \
  -p 5432:5432 \
  -d postgres:latest

echo "=== Waiting for PostgreSQL to start (10s) ==="
sleep 10

echo "=== Creating database ==="
docker exec -i $CONTAINER_NAME psql -U postgres -c "CREATE DATABASE \"$DATABASE_NAME\";"

echo "=== Creating sample tables ==="
docker exec -i $CONTAINER_NAME psql -U postgres -d $DATABASE_NAME << EOF
-- Create Customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    product_number VARCHAR(25) UNIQUE NOT NULL,
    color VARCHAR(15),
    standard_cost DECIMAL(10,2) NOT NULL,
    list_price DECIMAL(10,2) NOT NULL,
    size VARCHAR(5),
    weight DECIMAL(8,2),
    product_category_id INT,
    product_model_id INT,
    sell_start_date TIMESTAMP NOT NULL,
    sell_end_date TIMESTAMP,
    discontinued_date TIMESTAMP
);

-- Create Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ship_date TIMESTAMP,
    status SMALLINT NOT NULL DEFAULT 1,
    total_due DECIMAL(10,2) NOT NULL
);

-- Create OrderDetails table
CREATE TABLE order_details (
    order_detail_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    order_qty SMALLINT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    unit_price_discount DECIMAL(10,2) NOT NULL DEFAULT 0
);

-- Create a view for Sales Analysis
CREATE VIEW v_sales_analysis AS
SELECT 
    p.product_id,
    p.product_name,
    COUNT(od.order_detail_id) AS times_ordered,
    SUM(od.order_qty) AS total_quantity,
    SUM(od.order_qty * od.unit_price) AS total_revenue
FROM 
    products p
    LEFT JOIN order_details od ON p.product_id = od.product_id
GROUP BY 
    p.product_id, p.product_name;
EOF

echo "=== Adding sample data ==="
docker exec -i $CONTAINER_NAME psql -U postgres -d $DATABASE_NAME << EOF
-- Add some customers
INSERT INTO customers (first_name, last_name, email, phone)
VALUES 
    ('John', 'Smith', 'john.smith@example.com', '555-123-4567'),
    ('Jane', 'Doe', 'jane.doe@example.com', '555-987-6543'),
    ('Bob', 'Johnson', 'bob.johnson@example.com', '555-456-7890'),
    ('Alice', 'Williams', 'alice.williams@example.com', '555-789-0123'),
    ('David', 'Brown', 'david.brown@example.com', '555-234-5678');

-- Add some products
INSERT INTO products (product_name, product_number, color, standard_cost, list_price, size, weight, sell_start_date)
VALUES 
    ('Mountain Bike', 'BK-M68B-38', 'Black', 425.49, 699.99, '38', 25.5, CURRENT_TIMESTAMP),
    ('Road Bike', 'BK-R68R-44', 'Red', 325.99, 599.99, '44', 19.8, CURRENT_TIMESTAMP),
    ('Touring Bike', 'BK-T79Y-46', 'Yellow', 475.49, 799.99, '46', 27.2, CURRENT_TIMESTAMP),
    ('Cycling Cap', 'AC-C29B-S', 'Blue', 5.99, 12.99, 'S', 0.1, CURRENT_TIMESTAMP),
    ('Water Bottle', 'AC-W35S-O', 'Silver', 2.99, 6.99, 'O', 0.3, CURRENT_TIMESTAMP);

-- Add some orders and order details
INSERT INTO orders (customer_id, order_date, ship_date, status, total_due)
VALUES 
    (1, CURRENT_TIMESTAMP - INTERVAL '10 days', CURRENT_TIMESTAMP - INTERVAL '8 days', 5, 699.99),
    (2, CURRENT_TIMESTAMP - INTERVAL '8 days', CURRENT_TIMESTAMP - INTERVAL '6 days', 5, 612.98),
    (3, CURRENT_TIMESTAMP - INTERVAL '5 days', CURRENT_TIMESTAMP - INTERVAL '3 days', 5, 799.99),
    (1, CURRENT_TIMESTAMP - INTERVAL '3 days', NULL, 3, 19.98),
    (4, CURRENT_TIMESTAMP - INTERVAL '2 days', NULL, 2, 1299.98);

INSERT INTO order_details (order_id, product_id, order_qty, unit_price)
VALUES 
    (1, 1, 1, 699.99),
    (2, 2, 1, 599.99),
    (2, 4, 1, 12.99),
    (3, 3, 1, 799.99),
    (4, 5, 2, 6.99),
    (4, 4, 1, 12.99),
    (5, 1, 1, 699.99),
    (5, 3, 1, 599.99);
EOF

echo "=== Creating .env file ==="
cat > .env << EOL
# DataHub Connection (Required)
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_TOKEN=your_datahub_pat_token_here

# PostgreSQL Connection
PG_HOST_PORT=datahub_test_postgres:5432
PG_DATABASE=$DATABASE_NAME
PG_USER=postgres
PG_PASSWORD=$PG_PASSWORD

# Docker networking for tests
DATAHUB_TEST_ENV=true
DOCKER_COMPOSE_MODE=true

# Additional settings for DataHub
CONVERT_URNS_TO_LOWERCASE=true
INCLUDE_TABLES=true
INCLUDE_VIEWS=true
INCLUDE_JOBS=false
ENABLE_PROFILING=true
PROFILE_TABLE_LEVEL_ONLY=false
STATEFUL_INGESTION_ENABLED=true
REMOVE_STALE_METADATA=true

# Execution configuration
EXECUTOR_ID="default"
SCHEDULE_CRON="0 0 * * *"
SCHEDULE_TIMEZONE="UTC"
EOL

echo "=== Creating PostgreSQL recipe template ==="
mkdir -p configs
mkdir -p params
mkdir -p recipes/templates
mkdir -p recipes/instances/dev
cat > recipes/templates/postgres.yml << EOL
source:
  type: postgres
  config:
    # Connection configuration
    username: \${PG_USER}
    password: \${PG_PASSWORD}
    host_port: \${PG_HOST_PORT}
    database: \${PG_DATABASE}
    
    # Content configuration
    include_tables: \${INCLUDE_TABLES}
    include_views: \${INCLUDE_VIEWS}
    
    # Profiling settings
    profiling:
      enabled: \${ENABLE_PROFILING}
      profile_table_level_only: \${PROFILE_TABLE_LEVEL_ONLY}
      
    # Stateful ingestion for change detection
    stateful_ingestion:
      enabled: \${STATEFUL_INGESTION_ENABLED}
EOL

echo "=== Creating PostgreSQL recipe instance ==="
mkdir -p recipes/instances
cat > recipes/instances/dev/analytics-db.yml << EOL
# Instance-specific parameters for the Analytics Database
recipe_id: analytics-database-prod
recipe_type: postgres
description: "Production Analytics Database ingestion"

parameters:
  # Connection parameters
  PG_USER: "postgres"
  PG_HOST_PORT: "datahub_test_postgres:5432"
  PG_DATABASE: "$DATABASE_NAME"
  
  # Content configuration
  INCLUDE_TABLES: true
  INCLUDE_VIEWS: true
  
  # Stateful ingestion
  STATEFUL_INGESTION_ENABLED: true

  # Profiling configuration
  ENABLE_PROFILING: true
  PROFILE_TABLE_LEVEL_ONLY: false


# Secrets are referenced but not stored here
# They will be injected from environment variables
secret_references:
  - PG_PASSWORD
  - DATAHUB_GMS_URL
  - DATAHUB_TOKEN
EOL

echo "=== Setup Complete ==="
echo "PostgreSQL is running at: localhost:5432"
echo "Username: postgres"
echo "Password: $PG_PASSWORD"
echo "Database: $DATABASE_NAME"
echo "Sample .env file has been created"
echo ""
echo "To test your DataHub connection, update the DATAHUB_GMS_URL and DATAHUB_TOKEN in the .env file."
echo "To stop the container: docker stop $CONTAINER_NAME"
echo "To remove the container: docker rm $CONTAINER_NAME"