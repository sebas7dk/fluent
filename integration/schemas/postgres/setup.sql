-- Create the database and connect to it
\connect fluent

-- Test 1 table
DROP TABLE IF EXISTS test_1;
CREATE TABLE test_1(
  id SERIAL,
  name VARCHAR(255),
  total DECIMAL(10,2),
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE,
  deleted_at TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (id)
);

-- Test 2 table
DROP TABLE IF EXISTS test_2;
CREATE TABLE test_2(
  id SERIAL,
  test_id INT,
  is_active INT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE,
  deleted_at TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (id),
  FOREIGN KEY (test_id) REFERENCES test_1 (id)
);