"""

    Tasks

"""
from alsek.core.task import task
from examples.simple.singletons import broker


@task(broker, queue="math_ops", mechanism="thread", max_retries=3, timeout=3000)
def add(a: int, b: int) -> int:
    return a + b


if __name__ == "__main__":
    msg = add.generate(args=(1, 1))
    print(f"Task submitted. UUID: {msg.uuid}.")
