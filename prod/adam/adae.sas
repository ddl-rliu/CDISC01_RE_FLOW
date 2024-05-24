/******************************************************************************
* This is a dummy script that mocks the following use case:
*  1. Loading in the original SDTM ts.sas7bdat data & a processed ADSL data
*  2. Using that data to create the ADAE dataset
* 
*  For simplcity, we will simply merging the datasets together
*****************************************************************************/
options dlcreatedir;
libname inputs "/workflow/inputs"; /* All inputs live in this directory at workflow/inputs/<NAME OF INPUT> */ 
libname outputs "/workflow/outputs"; /* All outputs must go to this directory at workflow/inputs/<NAME OF OUTPUT>y */ 

x "cp /mnt/code/utilities/adae.sas7bdat /workflow/outputs/adam"

