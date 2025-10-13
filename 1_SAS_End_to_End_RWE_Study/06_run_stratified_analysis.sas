/******************************************************************************
* SCRIPT: 06_run_stratified_analysis.sas
*
* PURPOSE:
*   This script runs the stratified survival analyses for the NHS2 Greenspace
*   and Depression study. It systematically evaluates potential effect
*   modification by key demographic and environmental variables (e.g., Census Region).
*
* INPUTS:
*   - WORK.analysis_cohort (created by 01_etl.sas)
*
* OUTPUTS:
*   - An Excel file with formatted Hazard Ratios for all stratified models.
*   - An Excel file with p-values for interaction for all stratified models.
*
******************************************************************************/


/******************************************************************************
* STEP 1: DEFINE GLOBAL MACRO VARIABLES
******************************************************************************/

* --- Define Covariate Adjustment Sets ---;
* This section defines macro variables for the different adjustment sets
* required when modeling specific exposures. This avoids repeating long
* variable lists in the main macro calls.

%let base_adj = &bmi_ husbed husbm &racegrp_ current_smoke past_smoke alone ahei married nses popd;

%let stratadj_tree     = &base_adj grass_500m othgr_500m;
%let stratadj_grass    = &base_adj tree_500m othgr_500m;
%let stratadj_othgreen = &base_adj tree_500m grass_500m;


/******************************************************************************
* STEP 2: DEFINE CORE STRATIFIED ANALYSIS MACRO
******************************************************************************/

%macro run_stratified_model(out_suffix=, data=, strat_var=, event=, time=, adj_vars=, exposure=);
    /**************************************************************************
    * MACRO: run_stratified_model
    * PURPOSE: Executes a stratified Cox model for a single exposure, outcome,
    *          and stratification variable. It also runs a test for interaction
    *          and formats the outputs.
    **************************************************************************/

    * --- 1. Run the stratified Cox model using %mphreg9 ---;
    %mphreg9(
        data=&data,
        event=&event,
        time=&time,
        timevar=t01 t03 t05 t07 t09 t11 t13 t15 t17,
        contvar=&exposure nses ahei,
        tvar=period,
        agevar=agemo,
        strata=agemo,
        id=id,
        byvar=&strat_var,
        model1=&exposure &adj_vars,
        refcats=bmir racegrpr regr,
        outdat=work.output_raw
    );
    run;

    * --- 2. Calculate IQR-scaled HRs and format the results ---;
    proc means data=WORK.analysis_cohort noprint;
        var tree_500m grass_500m othgr_500m;
        output out=work.iqr_values p75= p25=;
    run;
    data work.iqr_values;
        set work.iqr_values;
        tree_500m_iqr  = tree_500m_p75 - tree_500m_p25;
        grass_500m_iqr = grass_500m_p75 - grass_500m_p25;
        othgr_500m_iqr = othgr_500m_p75 - othgr_500m_p25;
        keep tree_500m_iqr grass_500m_iqr othgr_500m_iqr;
    run;

    data work.output_formatted_&out_suffix;
        length model $64. exposure_var $32. outcome $32. strata $32.;
        if _n_ = 1 then set work.iqr_values;
        set work.output_raw;

        RR = cats(put(HazardRatio,4.2), ' (', put(LCL,4.2), ', ', put(UCL,4.2), ')');
        
        array vars {3} tree_500m grass_500m othgr_500m;
        array iqr  {3} tree_500m_iqr grass_500m_iqr othgr_500m_iqr;
        do i = 1 to dim(vars);
            if variable = vname(vars{i}) then do;
                HRIQR = exp(estimate * iqr{i});
                HRIQRL = exp((estimate - 1.96 * stderr) * iqr{i});
                HRIQRU = exp((estimate + 1.96 * stderr) * iqr{i});
                RRIQR = cats(put(round(HRIQR, .01), 4.2), ' (', put(round(HRIQRL, .01), 4.2), ',', put(round(HRIQRU, .01), 4.2), ')');
            end;
        end;

        model        = "&exposure._&event._&strat_var";
        exposure_var = "&exposure";
        outcome      = "&event";
        strata       = "&strat_var";
        keep model exposure_var outcome strata variable RR RRIQR;
    run;

    * --- 3. Run PROC PHREG again to get the p-value for interaction ---;
    title "Interaction Test: &exposure by &strat_var for outcome &event";
    proc phreg data=&data;
        ods output ModelANOVA=work.pval_raw;
        class &strat_var (ref=FIRST);
        strata agemo;
        model &time*&event(0) = &exposure|&strat_var &adj_vars / ties=efron rl type3(all);
    run;
    title;

    data work.pval_formatted_&out_suffix;
        length model $64. effect $64. exposure_var $32. outcome $32. strata $32.;
        set work.pval_raw;
        if upcase(Effect) contains upcase("&exposure*&strat_var");
        model        = "&exposure._&event._&strat_var._pval";
        exposure_var = "&exposure";
        outcome      = "&event";
        strata       = "&strat_var";
        keep model exposure_var outcome strata effect ProbChisq;
    run;

    * --- 4. Clean up temporary work datasets ---;
    proc datasets library=work nolist;
        delete output_raw pval_raw iqr_values;
    quit;
%mend run_stratified_model;


/******************************************************************************
* STEP 3: ORCHESTRATE AND EXECUTE ALL STRATIFIED ANALYSES
******************************************************************************/

%macro run_all_stratified;
    /**************************************************************************
    * MACRO: run_all_stratified
    * PURPOSE: This is the main driver macro. It defines all combinations of
    *          outcomes, exposures, and stratification variables, then loops
    *          through them to execute the full analysis.
    **************************************************************************/
    %local i j k out_counter;
    %local outcome_def outcome time;
    %local exposure_def exposure adj_vars;
    %local strat_var;

    %let outcomes_times = senscase|stime, bothcase|stime, depcase|deptime, adcase|stime;
    %let exposures_adj  = tree_500m|&stratadj_tree, grass_500m|&stratadj_grass, othgr_500m|&stratadj_othgreen;
    %let strat_vars     = &reg_; /* Add other stratification vars here, e.g., physact_q3 */

    %let out_counter = 1;

    /* Loop over each outcome definition */
    %let i = 1;
    %let outcome_def = %scan(&outcomes_times, &i, ',');
    %do %while (&outcome_def ne );
        %let outcome = %scan(&outcome_def, 1, '|');
        %let time = %scan(&outcome_def, 2, '|');

        /* Loop over each exposure and its adjustment set */
        %let j = 1;
        %let exposure_def = %scan(&exposures_adj, &j, ',');
        %do %while (&exposure_def ne );
            %let exposure = %scan(&exposure_def, 1, '|');
            %let adj_vars = %scan(&exposure_def, 2, '|');

            /* Loop over each stratification variable */
            %let k = 1;
            %let strat_var = %scan(&strat_vars, &k);
            %do %while (&strat_var ne );

                %put NOTE: RUNNING STRATIFIED MODEL (&out_counter): &exposure by &strat_var for &outcome;

                %run_stratified_model(
                    out_suffix = &out_counter,
                    data       = WORK.analysis_cohort,
                    strat_var  = &strat_var,
                    event      = &outcome,
                    time       = &time,
                    adj_vars   = &adj_vars,
                    exposure   = &exposure
                );

                %let k = %eval(&k + 1);
                %let strat_var = %scan(&strat_vars, &k);
                %let out_counter = %eval(&out_counter + 1);
            %end;

            %let j = %eval(&j + 1);
            %let exposure_def = %scan(&exposures_adj, &j, ',');
        %end;

        %let i = %eval(&i + 1);
        %let outcome_def = %scan(&outcomes_times, &i, ',');
    %end;

    /* --- Combine all results into final tables --- */
    data final_stratified_results;
        set work.output_formatted_:;
    run;

    data final_interaction_pvals;
        set work.pval_formatted_:;
    run;

    /* --- Export final tables to Excel --- */
    ods excel file="stratified_results_&sysdate..xlsx";
    
    ods excel options(sheet_name="Stratified HRs");
    title "Stratified Hazard Ratio Results by Region";
    proc print data=final_stratified_results;
    run;

    ods excel options(sheet_name="P-values for Interaction");
    title "P-Values for Interaction by Region";
    proc print data=final_interaction_pvals;
    run;

    ods excel close;
    title;

    /* --- Final cleanup --- */
    proc datasets lib=work nolist;
        delete output_formatted_: pval_formatted_: final_stratified_results final_interaction_pvals;
    quit;

%mend run_all_stratified;

/* Execute the entire stratified analysis workflow */
%put NOTE: --- STARTING STRATIFIED ANALYSIS ---;
%run_all_stratified;
%put NOTE: --- ALL STRATIFIED ANALYSES COMPLETE ---;