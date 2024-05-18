import os
from flytekit import workflow
from flytekit.types.file import FlyteFile,PDFFile
from utils.adam import create_adam_data
from utils.tfl import create_tfl_report
from typing import TypeVar

@workflow
def sce_workflow(sdtm_data_path: str) -> (PDFFile, PDFFile, PDFFile):
    """
    This script mocks a sample clinical trial using Domino Flows. 

    The input to this flow is the path to your SDTM data. You can point this to either your SDTM-BLIND dataset or your SDTM-UNBLIND dataset. The output to this flow are a series of TFL reports.

    To the run the workflow remotely, execute the following code in your terminal:
    
    pyflyte run --copy-all --remote workflow.py sce_workflow --sdtm_data_path "/mnt/imported/data/snapshots/sdtm-blind/1"

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
    # Create task that generates ADAE dataset. 
    adae = create_adam_data(
        name="ADAE", 
        command="prod/adam/adae.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl] # Note how this is the output from the previous task
    )
    # Create task that generates ADVS dataset. 
    advs = create_adam_data(
        name="ADVS", 
        command="prod/adam/advs.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl, adae]
    )
    # Create task that generates TFL report from T_POP table.
    t_pop = create_tfl_report(
        name="T_POP", 
        command="prod/tfl/t_pop.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[adsl]
    )
    # Create task that generates TFL report from T_AE_REL table.
    t_ae_rel = create_tfl_report(
        name="T_AE_REL", 
        command="prod/tfl/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[adae]
    )
    # Create task that generates TFL report from T_VSCAT table
    t_vscat = create_tfl_report(
        name="T_VSCAT", 
        command="prod/tfl/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[advs]
    )
    return t_pop, t_ae_rel, t_vscat