from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from parametrized_flow import etl_parent_flow    #importing from the locla py file

docker_block = DockerContainer.load("zoomcampprefect")

docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-flow",
    infrastructure=docker_block,
)

if __name__ == "__main__":
    docker_dep.apply()
    print("Deployment created successfully.")