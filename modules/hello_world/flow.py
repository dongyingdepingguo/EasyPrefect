from prefect import flow


@flow(name="Hello World")
def hello_world_flow() -> None:
    print("hello world")
