docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data:Z \
  -p 5432:5432 \
  docker.io/library/postgres:13


  #accessing db through bash

  #!/bin/bash

# Variables
DB_HOST="localhost"          # Hostname or IP of the container (localhost since mapped to host)
DB_PORT="5432"               # Port that is exposed (container port -> host port)
DB_USER="root"      # Your PostgreSQL username 
DB_PASSWORD="root"  # Your PostgreSQL password 
DB_NAME="ny_taxi"      # Your PostgreSQL database name 

# Export the password for psql to use without prompting
export PGPASSWORD=$DB_PASSWORD

# Connect using psql
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME
