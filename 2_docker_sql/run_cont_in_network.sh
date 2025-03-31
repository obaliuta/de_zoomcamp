
# network

#connecting existing podman network connect pg-network pg_database

# docker ps -- list all active containers

# docker ps -a - list all containers even stopped



docker network create pg-network

#  --network pg-network \ - after --p - issues an error

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network pg-network \
  --name pg_database \
  docker.io/library/postgres:13


docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network pg-network \
--name pgadmin \
docker.io/dpage/pgadmin4


