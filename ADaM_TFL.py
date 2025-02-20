import os
from flytekit import workflow
from flytekit.types.file import FlyteFile,PDFFile
from flytekit import WorkflowFailurePolicy
from utils.adam import create_adam_data
from utils.tfl import create_tfl_report
from utils.flyte import DominoTask, Input, Output
from typing import TypeVar

@workflow(failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
def Flow(sdtm_data_path: str):
    """
    This script mocks a sample clinical trial using Domino Flows. 

    The input to this flow is the path to your SDTM data. You can point this to either your SDTM-BLIND dataset or your SDTM-UNBLIND dataset. The output to this flow are a series of TFL reports.

    To the run the workflow remotely, execute the following code in your terminal:
    
    pyflyte run --copy-all --remote ADaM_TFL.py Flow --sdtm_data_path "/mnt/imported/data/snapshots/sdtm-blind/1"

    :param sdtm_data_path: The root directory of your SDTM dataset
    :return: A list of PDF files containing the TFL reports
    """
    # Create task that generates ADSL dataset. This will run a unique Domino job and return its outputs.
    adsl = create_adam_data(
        name="ADSL", 
        command="prod/adam/adsl.sas",
        environment="SAS Analytics Pro", # Optional parameter. If not set, then the default for the project will be used.
        hardware_tier= "Medium - [AWS US]", # Optional parameter. If not set, then the default for the project will be used.
        sdtm_data_path=sdtm_data_path # Note this this is simply the input value taken in from the command line argument
    )
    # Create task that generates ADAE dataset. 
    adae = create_adam_data(
        name="ADAE", 
        command="prod/adam/adae.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Medium - [AWS US]",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl] # Note how this is the output from the previous task
    )
    # Create task that generates ADVS dataset. 
    advs = create_adam_data(
        name="ADVS", 
        command="prod/adam/advs.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Medium - [AWS US]",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl, adae]
    )
    # Create task that generates ADCM dataset. 
    adcm = create_adam_data(
        name="ADCM", 
        command="prod/adam/adcm.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Medium - [AWS US]",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates ADEF dataset. 
    adef = create_adam_data(
        name="ADEF", 
        command="prod/adam/adef.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Medium - [AWS US]",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates ADLB dataset. 
    adlb = create_adam_data(
        name="ADLB", 
        command="prod/adam/adlb.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Medium - [AWS US]",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates ADMH dataset. 
    admh = create_adam_data(
        name="ADMH", 
        command="prod/adam/admh.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Medium - [AWS US]",
        sdtm_data_path=sdtm_data_path, 
        dependencies=[adsl]
    )
    # Create task that generates TFL report from T_POP table.
    t_pop = create_tfl_report(
       name="T_POP", 
        command="prod/tfl/t_pop.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[adsl]
    )
    # Create task that generates TFL report from T_AE_REL table.
    t_ae_rel = create_tfl_report(
        name="T_AE_REL", 
        command="prod/tfl/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[adae]
    )
    # Create task that generates TFL report from T_VSCAT table
    t_vscat = create_tfl_report(
        name="T_VSCAT", 
        command="prod/tfl/t_ae_rel.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[advs]
    )
    # Create task that generates TFL report from T_CONMED table
    t_conmed = create_tfl_report(
        name="T_CONMED", 
        command="prod/tfl/t_conmed.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[adcm]
    )
     # Create task that generates TFL report from T_DEMOG table
    t_demog = create_tfl_report(
        name="T_DEMOG", 
        command="prod/tfl/t_demog.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[adsl]
    )
     # Create task that generates TFL report from T_EFF table
    t_eff = create_tfl_report(
        name="T_EFF", 
        command="prod/tfl/t_eff.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[adef]
    )
     # Create task that generates TFL report from T_SAF table
    t_saf = create_tfl_report(
        name="T_SAF", 
        command="prod/tfl/t_saf.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[adsl, adae]
    )
      # Create task that generates TFL report from T_VITALS table
    t_vitals = create_tfl_report(
        name="T_VITALS", 
        command="prod/tfl/t_vitals.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[advs]
    )
     # Create task that generates TFL report from L_MEDHIST list
    l_medhist = create_tfl_report(
        name="L_MEDHIST", 
        command="prod/tfl/l_medhist.sas", 
        environment="SAS Analytics Pro",
        hardware_tier= "Small - [AWS US]",
        dependencies=[advs]
    )
    # Create AE Analysis Plot 
    ae_analysis = DominoTask(
        name="AE Analysis Plot in ggplot and R",
        command="prod/tfl/ae_analysis.R", 
        environment="GxP Validated R & Py", 
        hardware_tier="Small - [AWS US]",
        inputs=[
            Input(name="adae", type=FlyteFile[TypeVar("sas7bdat")], value=adae.data)
        ]
    )
    # Create SL Analysis R Plot 
    sl_analysis = DominoTask(
        name="Average Age Plot in ggplot and R",
        command="prod/tfl/sl_analysis.R", 
        environment="GxP Validated R & Py", 
        hardware_tier="Small - [AWS US]",
        inputs=[
            Input(name="adsl", type=FlyteFile[TypeVar("sas7bdat")], value=adsl.data)
        ]
    )
    # Combine all TFLs into a single PDF 
    merge_pdf = DominoTask(
        name="Merge TFL PDFs",
        command="utilities/merge_tfl.py", 
        environment="GxP Validated R & Py", 
        hardware_tier="Small - [AWS US]",
        inputs=[
            Input(name="t_pop", type=PDFFile, value=t_pop),
            Input(name="t_ae_rel", type=PDFFile, value=t_ae_rel),
            Input(name="t_conmed", type=PDFFile, value=t_conmed),
            Input(name="t_demog", type=PDFFile, value=t_demog),
            Input(name="t_eff", type=PDFFile, value=t_eff),
            Input(name="t_saf", type=PDFFile, value=t_saf),
            Input(name="t_vitals", type=PDFFile, value=t_vitals),
            Input(name="t_vscat", type=PDFFile, value=t_vscat),
            Input(name="l_medhist", type=PDFFile, value=l_medhist)
        ]
    )
    return merge_pdf