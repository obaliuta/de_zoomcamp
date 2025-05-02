from prefect.infrastructure.container import DockerContainer

docker_block = DockerContainer(
    image="obaliuta/zoomcamp_prefect:zoomcamp",
    image_pull_policy = "ALWAYS",
    auto_remove=True)

docker_block.save(
    name="zoomcamp_prefect",
    overwrite=True
)