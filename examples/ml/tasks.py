"""

    Tasks

"""

import torch
from torch import nn

from alsek.core.task import task
from examples.ml.singletons import broker, result_store


class Model(nn.Module):
    def __init__(self) -> None:
        super().__init__()
        self.linear = nn.Linear(10, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.linear(x)


model = Model().eval()


@task(broker, queue="ml_ops", result_store=result_store)
def predict(scale: float) -> float:
    tensor = torch.randn(10) * scale
    with torch.no_grad():
        prediction = model(tensor).item()
    return prediction


if __name__ == "__main__":
    msg = predict.generate(kwargs={"scale": 2.5})
    print(f"Task submitted. UUID: {msg.uuid}.")
