import os
from flytekit import workflow
from flytekit.types.file import FlyteFile,PDFFile
from flytekit import WorkflowFailurePolicy
from utils.adam import create_adam_data
from utils.tfl import create_tfl_report
from typing import TypeVar

@workflow(failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
def sdtm(sdtm_data_path: str) -> (PDFFile):
    """
    This script mocks a sample clinical trial using Domino Flows. 

    The input to this flow is the path to your SDTM data. You can point this to either your SDTM-BLIND dataset or your SDTM-UNBLIND dataset. The output to this flow are a series of TFL reports.

    To the run the workflow remotely, execute the following code in your terminal:
    
    pyflyte run --copy-all --remote workflow_2.py sdtm --sdtm_data_path "/mnt/imported/data/snapshots/sdtm-blind/1"

    :param sdtm_data_path: The root directory of your SDTM dataset
    :return: A list of PDF files containing the TFL reports
    """
    # Create task that generates ADSL dataset. This will run a unique Domino job and return its outputs.
    adsl = create_adam_data(
        name="ADSL", 
        command="prod/adam/adsl.sas",
        environment="SAS Analytics Pro", # Optional parameter. If not set, then the default for the project will be used.
        hardware_tier= "Small", # Optional parameter. If not set, then the default for the project will be used.
        sdtm_data_path=sdtm_data_path # Note this this is simply the input value taken in from the command line argument
    )
    # Create task that generates TFL report from T_POP table.
    t_pop = create_tfl_report(
        name="T_POP", 
        command="prod/tfl/t_pop.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[adsl]
    )
    return t_pop