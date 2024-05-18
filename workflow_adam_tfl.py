import os
from flytekit import workflow
from flytekit.types.file import FlyteFile,PDFFile
from utils.adam import create_adam_data
from utils.tfl import create_tfl_report
from typing import TypeVar, NamedTuple
from utils.adam import ADAM

adam_outputs = NamedTuple("adam_outputs", adsl=FlyteFile[TypeVar("sas7bdat")], adae=FlyteFile[TypeVar("sas7bdat")], advs=FlyteFile[TypeVar("sas7bdat")], adcm=FlyteFile[TypeVar("sas7bdat")], adef=FlyteFile[TypeVar("sas7bdat")], adlb=FlyteFile[TypeVar("sas7bdat")], admh=FlyteFile[TypeVar("sas7bdat")])
tfl_outputs = NamedTuple("tfl_outputs", t_ae_rel=PDFFile, t_vscat=PDFFile)

@workflow
def create_adam_datasets(sdtm_data_path: str) -> adam_outputs:
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
    # Create task that generates ADCM dataset. 
    adcm = create_adam_data(
        name="ADCM", 
        command="prod/adam/adcm.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates ADEF dataset. 
    adef = create_adam_data(
        name="ADEF", 
        command="prod/adam/adef.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates ADLB dataset. 
    adlb = create_adam_data(
        name="ADLB", 
        command="prod/adam/adlb.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates ADMH dataset. 
    admh = create_adam_data(
        name="ADMH", 
        command="prod/adam/admh.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )

    return adam_outputs(adsl=adsl.data, adae=adae.data, advs=advs.data, adcm=adcm.data, adef=adef.data, adlb=adlb.data, admh=admh.data)

@workflow
def create_tfl_reports(adsl: FlyteFile[TypeVar("sas7bdat")], adae: FlyteFile[TypeVar("sas7bdat")], advs: FlyteFile[TypeVar("sas7bdat")]) -> tfl_outputs:
    # Create task that generates TFL report from ADAE dataset.
    t_ae_rel = create_tfl_report(
        name="T_AE_REL", 
        command="sas -stdio prod/tfl/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[ADAM("adae", adae)]
    )
    # Create task that generates TFL report from ADVS dataset
    t_vscat = create_tfl_report(
        name="T_VSCAT", 
        command="sas -stdio prod/tfl/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small",
        dependencies=[ADAM("advs", advs)]
    )
    return tfl_outputs(t_ae_rel=t_ae_rel, t_vscat=t_vscat)

@workflow
def adam_tfl(sdtm_data_path: str) -> tfl_outputs:
    """
    This script mocks a sample clinical trial using Domino Flows. 

    The input to this flow is the path to your SDTM data. You can point this to either your SDTM-BLIND dataset or your SDTM-UNBLIND dataset. The output to this flow are a series of TFL reports.

    To the run the workflow remotely, execute the following code in your terminal:
    
    pyflyte run --copy-all --remote workflow_adam_tfl.py adam_tfl --sdtm_data_path "/mnt/imported/data/snapshots/sdtm-blind/1"

    :param sdtm_data_path: The root directory of your SDTM dataset
    :return: A list of PDF files containing the TFL reports
    """

    adam_datasets = create_adam_datasets(sdtm_data_path=sdtm_data_path)
    tfl_reports = create_tfl_reports(adsl=adam_datasets.adsl, adae=adam_datasets.adae, advs=adam_datasets.advs)

    return tfl_reports