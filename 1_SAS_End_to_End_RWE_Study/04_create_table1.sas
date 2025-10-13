/******************************************************************************
* SCRIPT: 04_create_table1.sas
*
* PURPOSE:
*   This script generates the descriptive baseline characteristics table (Table 1)
*   for the NHS2 Greenspace and Depression study. It creates an overall summary
*   table and tables stratified by quantiles of the primary exposure variables.
*
* INPUTS:
*   - WORK.analysis_cohort (created by 01_etl.sas)
*
* OUTPUTS:
*   - RTF files containing the formatted Table 1 outputs.
*
******************************************************************************/


* --- 1. Define Macro Variables for Table 1 ---;
* This section defines macro variables to hold the lists of variables
* for the %table1 macro. This is a best practice that makes the macro calls
* cleaner, easier to read, and simpler to maintain.

* List of all continuous variables for the table.
%let cont_vars = ageyr nses popd pm25 ppt ahei physact;

* List of all categorical variables for the table.
%let cat_vars = never_smoke current_smoke past_smoke racegrpr racegrp2 racegrp3
               alone husbr husbed husbm bmir bmi2 bmi3 bmim married
               regr reg2 reg3 reg4;

* Combined list of all variables.
%let all_vars = &cont_vars &cat_vars;


* --- 2. Generate Overall Cohort Characteristics Table ---;
* This first call to the %table1 macro generates the summary statistics
* for the entire cohort across all follow-up periods.

%put NOTE: Generating overall cohort characteristics table.;
%table1(
    data=WORK.analysis_cohort,
    agegroup=agegp,
    noexp=T,
    varlist=&all_vars,
    noadj=ageyr,
    multn=T,
    cat=&cat_vars,
    dec=1,
    file=n2_gsv_depre_table1_all_&sysdate.,
    rtftitle=Age-Standardized Characteristics of the Cohort Over All Follow-up
);


* --- 3. Generate Tables Stratified by Exposure Quantiles ---;
* This macro loop automates the creation of tables stratified by each of
* the primary exposure variables. This is far more efficient and less
* error-prone than writing separate macro calls for each exposure.

%macro generate_stratified_tables;
    /**************************************************************************
    * MACRO: generate_stratified_tables
    * PURPOSE: Loops through a list of exposure variables and calls the
    *          %table1 macro for each one to create stratified tables.
    **************************************************************************/
    %local i exposure exposure_label;
    %let exposures = qtree_500m qgrass_500m qothgr_500m;

    %let i = 1;
    %let exposure = %scan(&exposures, &i);

    %do %while (&exposure ne );
        %let exposure_label = %sysfunc(tranwrd(&exposure, q, ));
        %let exposure_label = %sysfunc(tranwrd(&exposure_label, _, ));

        %put NOTE: Generating stratified table for exposure: &exposure.;

        %table1(
            data=WORK.analysis_cohort,
            agegroup=agegp,
            exposure=&exposure,
            varlist=&all_vars,
            noadj=ageyr,
            multn=T,
            cat=&cat_vars,
            dec=1,
            file=n2_gsv_depre_table1_&exposure_label._quantiles_&sysdate.,
            rtftitle=Age-Standardized Characteristics by Quintiles of &exposure_label
        );

        %let i = %eval(&i + 1);
        %let exposure = %scan(&exposures, &i);
    %end;
%mend generate_stratified_tables;

/* Execute the macro to generate all stratified tables */
%generate_stratified_tables;

%put NOTE: Table 1 generation complete.;