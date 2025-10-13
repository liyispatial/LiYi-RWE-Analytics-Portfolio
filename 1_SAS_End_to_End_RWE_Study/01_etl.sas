/******************************************************************************
* SCRIPT: 01_etl.sas
*
* PURPOSE:
*   This script performs the primary data ingestion, cleaning, and transformation
*   (ETL) for the NHS2 Greenspace and Depression study. It reads in multiple
*   raw data sources (exposures, outcomes, covariates), merges them, and
*   creates a final, analysis-ready person-period dataset for survival analysis.
*
* WORKFLOW:
*   1. Ingest all exposure datasets (NDVI, GSV) and perform initial processing.
*   2. Ingest and define the depression outcome based on questionnaire data.
*   3. Ingest all covariate datasets (demographics, clinical, environmental).
*   4. Merge all data sources into a single "wide" file (one row per person).
*   5. Transform the wide file into a "long" person-period file for survival analysis.
*   6. Perform final data quality checks.
*
******************************************************************************/


/******************************************************************************
* STEP 0: CONFIGURATION & SETUP
******************************************************************************/

* --- Library & File Definitions ---;
* Define all libnames and filenames required for the project.
filename nhstools '/proj/nhsass/nhsas00/nhstools/sasautos/';
filename channing '/usr/local/channing/sasautos/';

libname readfmt   '/proj/nhsass/nhsas00/formatsv9/';
libname library   '/proj/nhsass/nhsas00/formats/';
libname der       '/proj/n2dats/n2dat.der/der8997/data';
libname nses      '/proj/n2dats/n2dat.der/nses8917';
libname death     '/proj/n2dats/n2_dat_cdx/deaths/';
libname diet      "/proj/hpalcs/hpalc0b/DIETSCORES/NHS2/";
libname geo       '/proj/nhairs/nhair0a/datasets';
libname prism     '/pc/nhair0a/PRISM_data/';
libname pollution "/pc/nhair0a/2019_AP_exposures/JeffY/predictions20220328052532";
libname gsv_raw   '/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv';

* --- Global Options ---;
options ls=125 ps=78 nocenter nonumber nodate formdlim='~';
options mautosource sasautos=(channing nhstools);
options fmtsearch=(readfmt);
options nofmterr;


/******************************************************************************
* STEP 1: INGEST & PROCESS EXPOSURE DATASETS
******************************************************************************/

* --- 1.1: Ingest Satellite-based NDVI Data ---;
%include '/proj/nhairs/nhair1w/landsat_ndvi/landsat_ndvi_nhs2_create.sas';

data nhs2_ndvi;
    set nhs2_ndvi;
    keep id ndvi1yr: ndvi2yr: cndvi1: cndvi2: ndvi1: ndvi2:;
run;

proc sort data=nhs2_ndvi; by id; run;


* --- 1.2: Ingest Street-View (GSV) Data ---;
%macro read_gsv(import=, importfile=);
    /**************************************************************************
    * MACRO: read_gsv
    * PURPOSE: Imports a CSV file and automatically converts all character
    *          variables (except 'id') to numeric type.
    **************************************************************************/
    proc import out=&import datafile=&importfile dbms=csv replace; getnames=YES; run;
    proc contents data=&import out=work.vars (keep=name type) noprint; run;
    proc sql noprint;
        select trim(left(name)), trim(left(name)) || '_n', trim(left(name)) || '_n' || '=' || trim(left(name))
        into :char_list separated by ' ', :num_list separated by ' ', :rename_list separated by ' '
        from work.vars where type=2 and upcase(name) ne 'ID';
    quit;
    %if %symexist(char_list) %then %do;
        data &import;
            set &import;
            array char_vars (*) $ &char_list;
            array num_vars (*) &num_list;
            do i = 1 to dim(char_vars); num_vars(i) = input(char_vars(i), best12.); end;
            drop i &char_list;
            rename &rename_list;
        run;
    %end;
    proc datasets lib=work nolist; delete vars; run;
%mend read_gsv;

/* Execute the macro for each GSV data file */
%read_gsv(import=gsv_1000m_p1, importfile="/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv/gsv_1000m/wide_gsv_1000m_p1_v2.csv");
%read_gsv(import=gsv_500m_p1,  importfile="/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv/gsv_500m/wide_gsv_500m_p1_v2.csv");
%read_gsv(import=gsv_100m_p1,  importfile="/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv/gsv_100m/wide_gsv_100m_p1_v2.csv");
%read_gsv(import=gsv_1000m_p2, importfile="/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv/gsv_1000m/wide_gsv_1000m_p2_v2.csv");
%read_gsv(import=gsv_500m_p2,  importfile="/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv/gsv_500m/wide_gsv_500m_p2_v2.csv");
%read_gsv(import=gsv_100m_p2,  importfile="/proj/nhairs/nhair2t/progs/n2_gsv_depre/data/gsv/gsv_100m/wide_gsv_100m_p2_v2.csv");

/* Merge all GSV parts into a single dataset */
data gsv_data;
    merge gsv_100m_p1 gsv_500m_p1 gsv_1000m_p1 gsv_100m_p2 gsv_500m_p2 gsv_1000m_p2;
    by id;
run;

proc datasets lib=work nolist; delete gsv_100m_p1--gsv_1000m_p2; run;


* --- 1.3: Calculate Cumulative Averages for GSV ---;
* NOTE: In a production environment, these long variable lists would be
* programmatically generated to reduce errors. For clarity in this portfolio
* piece, they are listed explicitly.
%let gsv_vars_in =
    gsv_c5_100m_01  gsv_c5_500m_01  gsv_c5_1000m_01  gsv_c10_100m_01  gsv_c10_500m_01  gsv_c10_1000m_01  gsv_c18_100m_01  gsv_c18_500m_01  gsv_c18_1000m_01  gsv_c30_100m_01  gsv_c30_500m_01  gsv_c30_1000m_01  gsv_c67_100m_01  gsv_c67_500m_01  gsv_c67_1000m_01  gsv_c73_100m_01  gsv_c73_500m_01  gsv_c73_1000m_01
    gsv_c5_100m_03  gsv_c5_500m_03  gsv_c5_1000m_03  gsv_c10_100m_03  gsv_c10_500m_03  gsv_c10_1000m_03  gsv_c18_100m_03  gsv_c18_500m_03  gsv_c18_1000m_03  gsv_c30_100m_03  gsv_c30_500m_03  gsv_c30_1000m_03  gsv_c67_100m_03  gsv_c67_500m_03  gsv_c67_1000m_03  gsv_c73_100m_03  gsv_c73_500m_03  gsv_c73_1000m_03
    gsv_c5_100m_05  gsv_c5_500m_05  gsv_c5_1000m_05  gsv_c10_100m_05  gsv_c10_500m_05  gsv_c10_1000m_05  gsv_c18_100m_05  gsv_c18_500m_05  gsv_c18_1000m_05  gsv_c30_100m_05  gsv_c30_500m_05  gsv_c30_1000m_05  gsv_c67_100m_05  gsv_c67_500m_05  gsv_c67_1000m_05  gsv_c73_100m_05  gsv_c73_500m_05  gsv_c73_1000m_05
    gsv_c5_100m_07  gsv_c5_500m_07  gsv_c5_1000m_07  gsv_c10_100m_07  gsv_c10_500m_07  gsv_c10_1000m_07  gsv_c18_100m_07  gsv_c18_500m_07  gsv_c18_1000m_07  gsv_c30_100m_07  gsv_c30_500m_07  gsv_c30_1000m_07  gsv_c67_100m_07  gsv_c67_500m_07  gsv_c67_1000m_07  gsv_c73_100m_07  gsv_c73_500m_07  gsv_c73_1000m_07
    gsv_c5_100m_09  gsv_c5_500m_09  gsv_c5_1000m_09  gsv_c10_100m_09  gsv_c10_500m_09  gsv_c10_1000m_09  gsv_c18_100m_09  gsv_c18_500m_09  gsv_c18_1000m_09  gsv_c30_100m_09  gsv_c30_500m_09  gsv_c30_1000m_09  gsv_c67_100m_09  gsv_c67_500m_09  gsv_c67_1000m_09  gsv_c73_100m_09  gsv_c73_500m_09  gsv_c73_1000m_09
    gsv_c5_100m_11  gsv_c5_500m_11  gsv_c5_1000m_11  gsv_c10_100m_11  gsv_c10_500m_11  gsv_c10_1000m_11  gsv_c18_100m_11  gsv_c18_500m_11  gsv_c18_1000m_11  gsv_c30_100m_11  gsv_c30_500m_11  gsv_c30_1000m_11  gsv_c67_100m_11  gsv_c67_500m_11  gsv_c67_1000m_11  gsv_c73_100m_11  gsv_c73_500m_11  gsv_c73_1000m_11
    gsv_c5_100m_13  gsv_c5_500m_13  gsv_c5_1000m_13  gsv_c10_100m_13  gsv_c10_500m_13  gsv_c10_1000m_13  gsv_c18_100m_13  gsv_c18_500m_13  gsv_c18_1000m_13  gsv_c30_100m_13  gsv_c30_500m_13  gsv_c30_1000m_13  gsv_c67_100m_13  gsv_c67_500m_13  gsv_c67_1000m_13  gsv_c73_100m_13  gsv_c73_500m_13  gsv_c73_1000m_13
    gsv_c5_100m_15  gsv_c5_500m_15  gsv_c5_1000m_15  gsv_c10_100m_15  gsv_c10_500m_15  gsv_c10_1000m_15  gsv_c18_100m_15  gsv_c18_500m_15  gsv_c18_1000m_15  gsv_c30_100m_15  gsv_c30_500m_15  gsv_c30_1000m_15  gsv_c67_100m_15  gsv_c67_500m_15  gsv_c67_1000m_15  gsv_c73_100m_15  gsv_c73_500m_15  gsv_c73_1000m_15
    gsv_c5_100m_17  gsv_c5_500m_17  gsv_c5_1000m_17  gsv_c10_100m_17  gsv_c10_500m_17  gsv_c10_1000m_17  gsv_c18_100m_17  gsv_c18_500m_17  gsv_c18_1000m_17  gsv_c30_100m_17  gsv_c30_500m_17  gsv_c30_1000m_17  gsv_c67_100m_17  gsv_c67_500m_17  gsv_c67_1000m_17  gsv_c73_100m_17  gsv_c73_500m_17  gsv_c73_1000m_17;

%let gsv_vars_out = %sysfunc(tranwrd(&gsv_vars_in, gsv, cgsv));

%include '/proj/nhchks/nhchk00/SAMPLE_CODES/cumavg.sas';

data gsv_data;
    set gsv_data;
    %cumavg(cycle=9, cyclevar=18, varin=&gsv_vars_in, varout=&gsv_vars_out);
run;


* --- 1.4: Impute Missing GSV and Create Composite Variables ---;
%macro impute_and_combine_gsv;
    /**************************************************************************
    * MACRO: impute_and_combine_gsv
    * PURPOSE: This macro encapsulates the logic for imputing missing GSV
    *          components and then creating the composite tree/grass/other
    *          greenspace variables. This avoids massive code repetition.
    **************************************************************************/
    data gsv_data_final;
        set gsv_data;

        /* Define arrays for all components that need imputation */
        array gsv_c5_100m{9} gsv_c5_100m_01-gsv_c5_100m_17;
        array gsv_c10_100m{9} gsv_c10_100m_01-gsv_c10_100m_17;
        array gsv_c18_100m{9} gsv_c18_100m_01-gsv_c18_100m_17;
        array gsv_c30_100m{9} gsv_c30_100m_01-gsv_c30_100m_17;
        array gsv_c67_100m{9} gsv_c67_100m_01-gsv_c67_100m_17;
        array gsv_c73_100m{9} gsv_c73_100m_01-gsv_c73_100m_17;
        /* ... (arrays for 500m and 1000m buffers) ... */

        /* Last Observation Carried Forward (LOCF) Imputation */
        do i = 2 to 9;
            if missing(gsv_c5_100m(i)) then gsv_c5_100m(i) = gsv_c5_100m(i-1);
            if missing(gsv_c10_100m(i)) then gsv_c10_100m(i) = gsv_c10_100m(i-1);
            if missing(gsv_c18_100m(i)) then gsv_c18_100m(i) = gsv_c18_100m(i-1);
            if missing(gsv_c30_100m(i)) then gsv_c30_100m(i) = gsv_c30_100m(i-1);
            if missing(gsv_c67_100m(i)) then gsv_c67_100m(i) = gsv_c67_100m(i-1);
            if missing(gsv_c73_100m(i)) then gsv_c73_100m(i) = gsv_c73_100m(i-1);
            /* ... (repeat for 500m and 1000m buffers) ... */
        end;

        /* Create composite greenspace variables (trees, grass, other) */
        array tree_100{9}  gsv_tree_100m_01  - gsv_tree_100m_17;
        array grass_100{9} gsv_grass_100m_01 - gsv_grass_100m_17;
        array othgr_100{9} gsv_othgr_100m_01 - gsv_othgr_100m_17;
        array tree_500{9}  gsv_tree_500m_01  - gsv_tree_500m_17;
        array grass_500{9} gsv_grass_500m_01 - gsv_grass_500m_17;
        array othgr_500{9} gsv_othgr_500m_01 - gsv_othgr_500m_17;
        array tree_1000{9}  gsv_tree_1000m_01  - gsv_tree_1000m_17;
        array grass_1000{9} gsv_grass_1000m_01 - gsv_grass_1000m_17;
        array othgr_1000{9} gsv_othgr_1000m_01 - gsv_othgr_1000m_17;

        do i = 1 to 9;
            tree_100(i)  = sum(gsv_c5_100m(i), gsv_c73_100m(i));
            grass_100(i) = gsv_c10_100m(i);
            othgr_100(i) = sum(gsv_c18_100m(i), gsv_c30_100m(i), gsv_c67_100m(i));
            tree_500(i)  = sum(gsv_c5_500m(i), gsv_c73_500m(i));
            grass_500(i) = gsv_c10_500m(i);
            othgr_500(i) = sum(gsv_c18_500m(i), gsv_c30_500m(i), gsv_c67_500m(i));
            tree_1000(i)  = sum(gsv_c5_1000m(i), gsv_c73_1000m(i));
            grass_1000(i) = gsv_c10_1000m(i);
            othgr_1000(i) = sum(gsv_c18_1000m(i), gsv_c30_1000m(i), gsv_c67_1000m(i));
        end;
    run;
%mend impute_and_combine_gsv;

%impute_and_combine_gsv;


/******************************************************************************
* STEP 2: INGEST & DEFINE OUTCOME (DEPRESSION)
******************************************************************************/

* --- 2.1: Ingest Raw Depression and Questionnaire Data ---;
%der8919(keep=retmo91--retmo17 irt01--irt17 birthday);
%nur93(keep=tcyc93--antid93 antd93 antd93all ret93);
%nur97(keep=przc97 zol97 paxil97 tcyc97 antid97 antd97 antd97all ret97);
%nur99(keep=tcyc99 przc99 zol99 paxil99 antid99 antd99 antd99all ret99);
%nur01(keep=mhi01 depdr01 nerv01 down01 calm01 energ01 blue01 worn01 happy01 prozc01 zol01 paxil01 celex01 antid01 antd01 ret01);
%nur03(keep=id q03 prozc03 zol03 paxil03 celex03 antd03 antid03 depr03 deprd03 ret03);
%nur05(keep=id q05 ssri05 antid05 depr05 deprd05 antd05 ret05, noformat=T);
%nur07(keep=id yr07 mo07 q07 ssri07 antd07 depr07 deprd07 ret07);
%nur09(keep=id yr09 mo09 q09 ssri09 antd09 depr09 deprd09 ret09);
%nur11(keep=id yr11 mo11 q11 ssri11 antd11 depr11 deprd11 ret11);
%nur13(keep=id yr13 mo13 q13 ssri13 antd13 depr13 deprd13 ret13);
%nur15(keep=id yr15 mo15 q15 ssri15 antd15 depr15 deprd15 ret15);
%nur17(keep=id yr17 mo17 q17 ssri17 antd17 depr17 deprd17 ret17);

data depression_raw;
    merge der8919 nur93 nur97 nur99 nur01 nur03 nur05 nur07 nur09 nur11 nur13 nur15 nur17;
    by id;
run;


* --- 2.2: Define Depression Diagnosis Date (cd_dtdx) ---;
data depression_defined;
    set depression_raw;

    /*
    ALGORITHM for Depression Diagnosis Date (dxdep):
    Assigns an estimated diagnosis date based on self-reported timeframes
    from biennial questionnaires. E.g., a report on the '03 questionnaire of
    a diagnosis between Jun '01 - May '03 is assigned the midpoint date.
    */
    dxdep=.;
    if depr03=1 then do;
        if deprd03=1 then dxdep=1218-(1218-irt01)/2;
        if deprd03=2 then dxdep=1230;
        if deprd03=3 then dxdep=irt03-(irt03-1242)/2;
        if deprd03=4 then dxdep=1230;
    end;
    else if depr05=1 then do;
        if deprd05=1 then dxdep=1242-(1242-irt01)/2;
        if deprd05=2 then dxdep=1254;
        if deprd05=3 then dxdep=irt05-(irt05-1266)/2;
        if deprd05=4 then dxdep=1254;
    end;
    else if depr07=1 then do;
        if deprd07=1 then dxdep=1266-(1266-irt01)/2;
        if deprd07=2 then dxdep=1278;
        if deprd07=3 then dxdep=irt07-(irt07-1290)/2;
        if deprd07=4 then dxdep=1278;
    end;
    else if depr09=1 then do;
        if deprd09=1 then dxdep=1290-(1290-irt01)/2;
        if deprd09=2 then dxdep=1302;
        if deprd09=3 then dxdep=irt09-(irt09-1314)/2;
        if deprd09=4 then dxdep=1302;
    end;
    else if depr11=1 then do;
        if deprd11=1 then dxdep=1314-(1314-irt01)/2;
        if deprd11=2 then dxdep=1326;
        if deprd11=3 then dxdep=irt11-(irt11-1338)/2;
        if deprd11=4 then dxdep=1326;
    end;
    else if depr13=1 then do;
        if deprd13=1 then dxdep=1338-(1338-irt01)/2;
        if deprd13=2 then dxdep=1350;
        if deprd13=3 then dxdep=irt13-(irt13-1362)/2;
        if deprd13=4 then dxdep=1350;
    end;
    else if depr15=1 then do;
        if deprd15=1 then dxdep=1362-(1362-irt01)/2;
        if deprd15=2 then dxdep=1374;
        if deprd15=3 then dxdep=irt15-(irt15-1386)/2;
        if deprd15=4 then dxdep=1374;
    end;
    else if depr17=1 then do;
        if deprd17=1 then dxdep=1386-(1386-irt01)/2;
        if deprd17=2 then dxdep=1398;
        if deprd17=3 then dxdep=irt17-(irt17-1410)/2;
        if deprd17=4 then dxdep=1398;
    end;

    /* ALGORITHM for Antidepressant Use Date (rxdep): Inferred as 12 months
       prior to the return date of the first questionnaire reporting use. */
    rxdep=.;
    if antd01=1 then rxdep=irt01-12;
    else if antd03=1 then rxdep=irt03-12;
    else if antd05=1 then rxdep=irt05-12;
    else if antd07=1 then rxdep=irt07-12;
    else if antd09=1 then rxdep=irt09-12;
    else if antd11=1 then rxdep=irt11-12;
    else if antd13=1 then rxdep=irt13-12;
    else if antd15=1 then rxdep=irt15-12;
    else if antd17=1 then rxdep=irt17-12;

    /* FINAL OUTCOME: Date of incident depression is the EARLIEST of either
       the self-reported diagnosis date or the inferred antidepressant use date. */
    cd_dtdx = min(dxdep, rxdep);

    keep id cd_dtdx;
run;


/******************************************************************************
* STEP 3: INGEST COVARIATE DATASETS
******************************************************************************/

* --- 3.1: Ingest All Raw Covariate Datasets ---;
%nses8917();
%deadff(keep=id deadmonth dtdth);
%ahei2010_9115();
%act8917();
%nur89(keep=mob89 yob89 db89 hrt89 mi89 str89 can89 brcn89 ocan89 mel89 bcc89 mar89 marry89);
%nur91(keep=mel91 scc91 bcc91 brcn91 ocan91 can91 db91 mi91 ang91 str91 hrt91);
%nur93(keep=mel93 scc93 bcc93 brcn93 ocan93 can93 db93 mi93 ang91 str93 hrt93 marry93 div93 widow93 nvmar93 q37pt93 alone93);
%nur95(keep=mel95 scc95 bcc95 brcn95 ocan95 can95 db95 mi95 ang95 str95 hrt95);
%nur97(keep=mvit97 mel97 scc97 bcc97 brcn97 ocan97 can97 hrt97 db97 mi97 ang97 str97);
%ses97(keep=marry97 div97 widow97 nvmar97 alone97);
%nur99(keep=mel99 scc99 bcc99 brcn99 ocan99 can99 db99 mi99 ang99 str99 hrt99 wt99 husbe99);
%nur01(keep=mvit01 sleep01 snore01 marry01 divor01 widow01 nvmar01 separ01 q27pt01 mar01 mel01 scc01 bcc01 brcn01 ocan01 can01 db01 mi01 ang01 str01 hrt01 alone01 depre01 married01);
%nur03(keep=mel03 scc03 bcc03 brcn03 ocan03 can03 db03 mi03 ang03 str03 hrt03 depr03 deprd03 prozc03 zol03 paxil03 celex03 antid03);
%nur05(keep=mel05 scc05 bcc05 brcn05 ocan05 can05 db05 mi05 ang05 str05 hrt05 alone05);
%nur07(keep=smk07 mel07 scc07 bcc07 brcn07 ocan07 can07 db07 mi07 ang07 str07 hrt07);
%nur09(keep=smk09 mel09 scc09 bcc09 brcn09 ocan09 can09 db09 mi09 ang09 str09 hrt09 alone09);
%nur11(keep=smk11 mel11 scc11 bcc11 brcn11 ocan11 can11 db11 mi11 ang11 str11 hrt11);
%nur13(keep=smk13 mel13 scc13 bcc13 brcn13 ocan13 can13 db13 mi13 ang13 str13 hrt13 alone13);
%nur15(keep=smk15 mel15 scc15 bcc15 brcn15 ocan15 can15 db15 mi15 ang15 str15 hrt15);
%nur17(keep=smk17 mel17 scc17 bcc17 brcn17 ocan17 can17 db17 mi17 ang17 str17 hrt17 alone17);
%n91_nts(keep=id alco91n);
%n95_nts(keep=id alco95n);
%n99_nts(keep=id alco99n);
%n03_nts(keep=id alco03n);
%n07_nts(keep=id alco07n);
%n11_nts(keep=id alco11n);
%n15_nts(keep=id alco15n);

* --- 3.2: Merge All Datasets into a Single Wide File ---;
data cohort_wide;
    merge
        depression_defined (in=a)
        nhs2_ndvi
        gsv_data_final
        pollution
        ppt
        der8919 (in=mstr)
        state
        nses8917
        nur89 nur91 nur93 nur95 nur97 ses97 nur99 nur01 nur03 nur05 nur07 nur09 nur11 nur13 nur15 nur17
        act8917 ahei2010_9115
        n91_nts n95_nts n99_nts n03_nts n07_nts n11_nts n15_nts
        deadff
    ;
    by id;

    if a and mstr;
    if id=. then delete;

    if 0 < dtdth < 9999 then dead=1; else dead=0;
run;


/******************************************************************************
* STEP 4: CREATE PERSON-PERIOD ANALYSIS DATASET
******************************************************************************/

data analysis_cohort;
    set cohort_wide;

    * --- 4.1: Define Arrays for Longitudinal Variables ---;
    array retmoarr{10} retmo01 retmo03 retmo05 retmo07 retmo09 retmo11 retmo13 retmo15 retmo17 9999;
    array agex{9}      age01 age03 age05 age07 age09 age11 age13 age15 age17;
    array bmiarr{9}    bmi01 bmi03 bmi05 bmi07 bmi09 bmi11 bmi13 bmi15 bmi17;
    array smkdrarr{9}  smkdr01 smkdr03 smkdr05 smkdr07 smkdr09 smkdr11 smkdr13 smkdr15 smkdr17;
    array tree_500m_arr{9} gsv_tree_500m_01 - gsv_tree_500m_17;
    array grass_500m_arr{9} gsv_grass_500m_01 - gsv_grass_500m_17;
    array othgr_500m_arr{9} gsv_othgr_500m_01 - gsv_othgr_500m_17;
    array tree_100m_arr{9} gsv_tree_100m_01 - gsv_tree_100m_17;
    array grass_100m_arr{9} gsv_grass_100m_01 - gsv_grass_100m_17;
    array othgr_100m_arr{9} gsv_othgr_100m_01 - gsv_othgr_100m_17;
    array tree_1000m_arr{9} gsv_tree_1000m_01 - gsv_tree_1000m_17;
    array grass_1000m_arr{9} gsv_grass_1000m_01 - gsv_grass_1000m_17;
    array othgr_1000m_arr{9} gsv_othgr_1000m_01 - gsv_othgr_1000m_17;
    array canarr{9} can01 can03 can05 can07 can09 can11 can13 can15 can17;
    array dbarr{9} db01 db03 db05 db07 db09 db11 db13 db15 db17;
    array hrtarr{9} hrt01 hrt03 hrt05 hrt07 hrt09 hrt11 hrt13 hrt15 hrt17;
    array mnpstarr{9} mnpst01 mnpst03 mnpst05 mnpst07 mnpst09 mnpst11 mnpst13 mnpst15 mnpst17;
    array alonearr{9} alone01 alone01 alone05 alone05 alone09 alone09 alone13 alone13 alone17;
    array nsesarr{9} nSES_01 nSES_03 nSES_05 nSES_07 nSES_09 nSES_11 nSES_13 nSES_15 nSES_17;
    array actarr{9} act01m act01m act05m act05m act09m act09m act13m act13m act17m;

    * --- 4.2: Create Person-Period Records via DO Loop ---;
    * This loop transforms the data from wide to long format, creating one
    * record per person for each 2-year follow-up period they are at risk.
    do period = 1 to 9;

        * Define start and end times for the current period.
        start_time = retmoarr{period};
        end_time   = retmoarr{period+1};

        * Define the outcome for this period (incident depression).
        depcase = (start_time < cd_dtdx <= end_time);
        
        * Calculate person-time (stime) for this period.
        stime = end_time - start_time;
        if depcase then stime = cd_dtdx - start_time;
        if start_time <= dtdth < end_time then stime = min(stime, dtdth - start_time);

        * Assign time-varying covariates for this period.
        ageyr = agex{period};
        bmicont = bmiarr{period};
        smoke = smkdrarr{period};
        tree_500m = tree_500m_arr{period};
        grass_500m = grass_500m_arr{period};
        othgr_500m = othgr_500m_arr{period};
        tree_100m = tree_100m_arr{period};
        grass_100m = grass_100m_arr{period};
        othgr_100m = othgr_100m_arr{period};
        tree_1000m = tree_1000m_arr{period};
        grass_1000m = grass_1000m_arr{period};
        othgr_1000m = othgr_1000m_arr{period};
        cancer = canarr{period};
        diabetes = dbarr{period};
        heart = hrtarr{period};
        mnpst = mnpstarr{period};
        alone = alonearr{period};
        nses = nsesarr{period};
        physact = actarr{period};

        * Apply censoring logic for this period.
        if (cd_dtdx <= start_time) or (dtdth <= start_time) then continue;
        if missing(tree_500m) or missing(bmicont) or missing(smoke) then continue;

        output; /* Write one record to the output dataset */
    end;

    * --- 4.3: Final Variable Creation and Cleanup ---;
    if 0 < ageyr < 30 then agegp = 1;
    else if 30 <= ageyr < 35 then agegp = 2;
    else if 35 <= ageyr < 40 then agegp = 3;
    else if 40 <= ageyr < 45 then agegp = 4;
    else if 45 <= ageyr < 50 then agegp = 5;
    else if 50 <= ageyr < 55 then agegp = 6;
    else if ageyr >=55 then agegp = 7;

    keep id period start_time stime depcase ageyr bmicont smoke tree_500m grass_500m othgr_500m
         tree_100m grass_100m othgr_100m tree_1000m grass_1000m othgr_1000m
         cancer diabetes heart mnpst alone nses physact agegp;
run;


/******************************************************************************
* STEP 5: FINAL DATA CHECKS
******************************************************************************/

proc means data=analysis_cohort n nmiss mean median min max;
    title "Final Data Quality Check of Analysis-Ready Dataset";
run;
title;

proc freq data=analysis_cohort;
    tables period depcase agegp smoke;
    title "Distribution of Key Categorical Variables";
run;
title;