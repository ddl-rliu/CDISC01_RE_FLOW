import os
from .flyte import DominoTask, Input, Output
from .adam import ADAM
from typing import List, TypeVar
from flytekitplugins.domino.task import DominoJobConfig, DominoJobTask
from flytekit import workflow, task
from flytekit.types.file import FlyteFile,PDFFile
from flytekit.types.directory import FlyteDirectory

def create_tfl_report(
    name: str, 
    command: str, 
    dependencies: List[ADAM],
    environment: str = None,
    hardware_tier: str = None
) -> FlyteFile[TypeVar("pdf")]:
    """
    This method provides a standard interface for creating a TFL report 

    :param name: The name in which to give the report. This is used to generate the step name.
    :param command: The command to execute for generating the report
    :param environment: The name of the environment you want to use. If not specified, the project default will be used.
    :param hardware_tier: The name of the hardware tier you want to use. If not specified, the project default will be used.
    :param adam_dataset: The processed ADAM dataset to use for generating the report
    :return: A PDF files containing the final TFL report
    """
    # Define inputs
    inputs = []
    for dataset in dependencies:
        inputs.append(Input(name=dataset.filename, type=FlyteFile[TypeVar("sas7bdat")], value=dataset.data))

    # Define outputs
    outputs = [Output(name="report", type=FlyteFile[TypeVar("pdf")])]

    results = DominoTask(
        name=f"Create {name} report",
        command=command, 
        environment=environment,
        hardware_tier=hardware_tier,
        inputs=inputs,
        outputs=outputs
    )

    return results["report"]



 