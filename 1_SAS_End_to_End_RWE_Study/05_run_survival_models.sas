/******************************************************************************
* SCRIPT: 05_run_survival_models.sas
*
* PURPOSE:
*   This script runs the primary and sensitivity survival analyses for the
*   NHS2 Greenspace and Depression study. It uses a modular, macro-driven
*   approach to systematically execute a series of Cox proportional hazards
*   models for different exposure definitions and outcomes.
*
* INPUTS:
*   - WORK.analysis_cohort (created by 01_etl.sas)
*
* OUTPUTS:
*   - An Excel file containing the formatted results (Hazard Ratios and 95% CIs)
*     for all models, exposures, and outcomes.
*
******************************************************************************/


/******************************************************************************
* STEP 1: DEFINE GLOBAL MACRO VARIABLES
******************************************************************************/

* --- Define Model Covariate Lists ---;
* This section defines macro variables to hold the lists of covariates for
* each adjustment level. This is a best practice that makes the model
* specifications clean, consistent, and easy to maintain.

%let model1_covars = ; /* Age-adjusted model (age is handled by mphreg9) */
%let model2_covars = &bmi_ husbed husbm &racegrp_ current_smoke past_smoke alone ahei married;
%let model3_covars = &model2_covars nses popd;
%let model4_covars = &model3_covars &reg_;
%let model5_covars = &model3_covars physact;
%let model6_covars = &model3_covars pm25;
%let model7_covars = &model3_covars ppt;
%let model8_covars = &model3_covars division;
%let model9_covars = &bmi_ husbed husbm &racegrp_ alone married nses popd; /* Excludes smoking and diet */


/******************************************************************************
* STEP 2: DEFINE CORE ANALYSIS MACROS
******************************************************************************/

%macro run_cox_models(data=, event=, time=, model_name=, exposures=);
    /**************************************************************************
    * MACRO: run_cox_models
    * PURPOSE: Executes a series of 9 pre-specified Cox models for a given
    *          set of exposures using the %mphreg9 wrapper macro.
    **************************************************************************/

    %mphreg9(
        data=&data,
        event=&event,
        time=&time,
        timevar=t01 t03 t05 t07 t09 t11 t13 t15 t17,
        id=id,
        agevar=agemo,
        contvar=&exposures nses ahei,
        tvar=period,

        model1= &exposures &model1_covars,
        model2= &exposures &model2_covars,
        model3= &exposures &model3_covars,
        model4= &exposures &model4_covars,
        model5= &exposures &model5_covars,
        model6= &exposures &model6_covars,
        model7= &exposures &model7_covars,
        model8= &exposures &model8_covars,
        model9= &exposures &model9_covars,

        refcats=bmir racegrpr regr,
        outdat=&model_name._raw_results
    );
%mend run_cox_models;


%macro calculate_iqr_hr(in_ds=, out_ds=, model_name=, event=);
    /**************************************************************************
    * MACRO: calculate_iqr_hr
    * PURPOSE: Calculates Hazard Ratios for the interquartile range (IQR) of
    *          continuous exposure variables and formats the final output table.
    **************************************************************************/

    /* First, calculate IQR for all potential continuous exposure variables */
    proc means data=WORK.analysis_cohort noprint;
        var tree_100m--othgr_1000m c_tree_500m--c_othgr_500m ndvi_270m ndvi_1230m;
        output out=work.iqr_values p75= p25=;
    run;

    data work.iqr_values;
        set work.iqr_values;
        array p75(*) _numeric_ (of _all_);
        array p25(*) _numeric_ (of _all_);
        array iqr(*) _numeric_ (of _all_);
        do i = 1 to dim(p75);
            iqr(i) = p75(i) - p25(i);
        end;
        /* Clean up variable names for merging */
        rename %do i=1 %to %sysfunc(countw(%sysfunc(getvarlist(work.iqr_values))));
                   %let var = %scan(%sysfunc(getvarlist(work.iqr_values)),&i);
                   &var=iqr_&var
               %end;;
    run;

    /* Calculate and format IQR-scaled HRs */
    data &out_ds;
        if _n_ = 1 then set work.iqr_values;
        set &in_ds;

        array vars(*) _numeric_ (of _all_);
        array iqr(*) _numeric_ (of _all_);

        do i = 1 to dim(vars);
            if variable = vname(vars(i)) then do;
                HRIQR = exp(estimate * iqr(i));
                HRIQRL = exp((estimate - 1.96 * stderr) * iqr(i));
                HRIQRU = exp((estimate + 1.96 * stderr) * iqr(i));
                RRIQR = cats(put(round(HRIQR, .01), 4.2), ' (', put(round(HRIQRL, .01), 4.2), ',', put(round(HRIQRU, .01), 4.2), ')');
            end;
        end;

        RR = cats(put(round(HazardRatio,.01),4.2), ' (', put(round(LCL,.01),4.2), ', ', put(round(UCL,.01),4.2), ')');
        beta = put(Estimate, 9.6);
        see = put(StdErr, 9.6);

        length model $64.;
        select(modelno);
            when (1) model='Model 1: Age-adjusted';
            when (2) model='Model 2: + Individual-level covariates';
            when (3) model='Model 3: + Neighborhood-level covariates (nSES, Pop Density)';
            when (4) model='Model 4: + Census Region';
            when (5) model='Model 5: + Physical Activity';
            when (6) model='Model 6: + PM2.5';
            when (7) model='Model 7: + Precipitation';
            when (8) model='Model 8: + Census Division';
            when (9) model='Model 9: Model 3 excluding smoking and diet';
            otherwise;
        end;

        event_desc = "&event.";
        model_name = "&model_name.";
        keep event_desc model_name variable model RR RRIQR ProbChisq beta see;
    run;
%mend calculate_iqr_hr;


%macro orchestrate_analysis(data=, event=, time=);
    /**************************************************************************
    * MACRO: orchestrate_analysis
    * PURPOSE: This is the main driver macro. It defines all exposure sets
    *          (main models, sensitivity analyses) and loops through them,
    *          calling the modeling and formatting macros for each one.
    **************************************************************************/
    %local i model_def model_name exposures;

    /* Define all models to be run in a single, clear location. */
    /* Format: model_name | exposure_variable_list */
    %let all_models =
        main500      | tree_500m grass_500m othgr_500m,
        main100      | tree_100m grass_100m othgr_100m,
        main1000     | tree_1000m grass_1000m othgr_1000m,
        sens_ndvi1   | ndvi_270m,
        sens_ndvi2   | ndvi_1230m,
        sens_combo1  | ndvi_270m tree_500m grass_500m othgr_500m,
        sens_combo2  | ndvi_1230m tree_1000m grass_1000m othgr_1000m,
        sens_cumavg  | c_tree_500m c_grass_500m c_othgr_500m
    ;

    %let i = 1;
    %let model_def = %scan(&all_models, &i, ',');

    %do %while (&model_def ne );
        %let model_name = %scan(&model_def, 1, '|');
        %let exposures = %scan(&model_def, 2, '|');

        %put NOTE: RUNNING MODELS FOR: &model_name with event &event.;

        /* 1. Run the 9 Cox models for the current exposure set */
        %run_cox_models(
            data=&data,
            event=&event,
            time=&time,
            model_name=&model_name,
            exposures=&exposures
        );

        /* 2. Calculate IQR HRs and format the results */
        %calculate_iqr_hr(
            in_ds=&model_name._raw_results,
            out_ds=&model_name._final_results,
            model_name=&model_name,
            event=&event
        );

        /* 3. Append the final results to a master dataset for this outcome */
        proc append base=all_results_&event data=&model_name._final_results;
        run;

        %let i = %eval(&i + 1);
        %let model_def = %scan(&all_models, &i, ',');
    %end;

    /* 4. Export the master results table for this outcome to Excel */
    ods excel file="results_&event._&sysdate..xlsx";
    title "Full Formatted Results for Outcome: &event";
    proc print data=all_results_&event.;
    run;
    title;
    ods excel close;

    /* 5. Clean up intermediate datasets */
    proc datasets lib=work nolist;
        delete %do i=1 %to %sysfunc(countw(&all_models, ','));
                   %let model_name = %scan(%scan(&all_models, &i, ','), 1, '|');
                   &model_name._raw_results &model_name._final_results
               %end;;
    run;

%mend orchestrate_analysis;


/******************************************************************************
* STEP 3: EXECUTE ANALYSIS FOR ALL OUTCOMES
******************************************************************************/

* This final section loops through each pre-defined outcome (depression definition)
* and calls the main orchestration macro. This makes the entire script
* fully automated.

%put NOTE: --- STARTING MAIN ANALYSIS ---;

%macro run_all_outcomes;
    %local j outcome time;
    %let outcomes_times =
        senscase | stime,
        bothcase | senstime,
        depcase  | deptime,
        adcase   | stime
    ;

    %let j = 1;
    %let outcome_def = %scan(&outcomes_times, &j, ',');

    %do %while (&outcome_def ne );
        %let outcome = %scan(&outcome_def, 1, '|');
        %let time = %scan(&outcome_def, 2, '|');

        %put NOTE: === ORCHESTRATING ANALYSIS FOR OUTCOME: &outcome ===;
        %orchestrate_analysis(data=WORK.analysis_cohort, event=&outcome, time=&time);

        %let j = %eval(&j + 1);
        %let outcome_def = %scan(&outcomes_times, &j, ',');
    %end;
%mend run_all_outcomes;

/* Execute the entire analysis workflow */
%run_all_outcomes;

%put NOTE: --- ALL ANALYSES COMPLETE ---;