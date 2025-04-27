"""

    Tasks

"""

from alsek.core.task import task
from examples.simple.singletons import broker


@task(
    broker,
    queue="math_ops",
    mechanism="thread",
    max_retries=3,
    timeout=3000,
)
def divide(a: int, b: int) -> float:
    if b == 0:
        raise ZeroDivisionError()
    return a / b


@task(
    broker,
    queue="math_ops",
    mechanism="thread",
    max_retries=3,
    timeout=3000,
)
def multiply(a: int, b: int) -> float:
    return a * b


if __name__ == "__main__":
    msg = divide.generate(args=(1, 2))
    print(f"Divide task submitted. UUID: {msg.uuid}.")

    msg = multiply.generate(args=(1, 2))
    print(f"Multiply task submitted. UUID: {msg.uuid}.")
