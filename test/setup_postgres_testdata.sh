#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Setting up test data in PostgreSQL ===${NC}"

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed or not in PATH${NC}"
    exit 1
fi

# Check if DataHub Quickstart is running
POSTGRES_CONTAINER=$(docker ps -q --filter "name=postgres" --filter "ancestor=postgres")

if [[ -z "$POSTGRES_CONTAINER" ]]; then
    echo -e "${YELLOW}Looking for DataHub Quickstart postgres container...${NC}"
    POSTGRES_CONTAINER=$(docker ps -q --filter "label=com.docker.compose.service=postgres")
fi

if [[ -z "$POSTGRES_CONTAINER" ]]; then
    echo -e "${RED}PostgreSQL container not found. Is DataHub Quickstart running?${NC}"
    echo -e "${YELLOW}You can start it with:${NC}"
    echo "    docker-compose -f docker-compose.yml -p datahub up -d"
    exit 1
fi

echo -e "${GREEN}Found PostgreSQL container: $POSTGRES_CONTAINER${NC}"

# Create the SQL script to initialize test data
cat > /tmp/init_test_data.sql << 'EOL'
-- Create a test schema
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create a test table
CREATE TABLE IF NOT EXISTS test_schema.test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some test data
INSERT INTO test_schema.test_table (name, description) VALUES
('Test Record 1', 'This is a test record for DataHub ingestion'),
('Test Record 2', 'Another test record for DataHub ingestion'),
('Test Record 3', 'Yet another test record for DataHub ingestion');

-- Create a test view
CREATE OR REPLACE VIEW test_schema.test_view AS
SELECT id, name, created_at
FROM test_schema.test_table
WHERE description LIKE '%DataHub%';

-- Create a function for testing
CREATE OR REPLACE FUNCTION test_schema.get_test_records(search_term TEXT)
RETURNS TABLE (id INT, name VARCHAR, description TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT t.id, t.name, t.description
    FROM test_schema.test_table t
    WHERE t.description ILIKE '%' || search_term || '%';
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA test_schema TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA test_schema TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA test_schema TO postgres;
EOL

# Copy the SQL file to the container
echo -e "${YELLOW}Copying SQL init script to container...${NC}"
docker cp /tmp/init_test_data.sql $POSTGRES_CONTAINER:/tmp/init_test_data.sql

# Execute the SQL script in the container
echo -e "${YELLOW}Executing SQL script to create test data...${NC}"
docker exec -i $POSTGRES_CONTAINER psql -U postgres -d postgres -f /tmp/init_test_data.sql

# Check if successful
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}✅ Successfully created test data in PostgreSQL${NC}"
    
    # Verify by counting records
    echo -e "${YELLOW}Verifying test data...${NC}"
    COUNT=$(docker exec -i $POSTGRES_CONTAINER psql -U postgres -d postgres -tAc "SELECT COUNT(*) FROM test_schema.test_table")
    echo -e "${GREEN}Found $COUNT records in test_schema.test_table${NC}"
    
    echo -e "${GREEN}Test data is ready for DataHub ingestion${NC}"
else
    echo -e "${RED}❌ Failed to create test data in PostgreSQL${NC}"
    exit 1
fi

# Clean up
rm /tmp/init_test_data.sql
echo -e "${GREEN}Done!${NC}" 