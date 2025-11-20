# -----------------------------------------------------------------------------
# Library setup
# -----------------------------------------------------------------------------
.libPaths("/usr/local/lib/R/site-library")

library(targets)
library(tarchetypes)
library(readxl)

# -----------------------------------------------------------------------------
# Global options for targets
# -----------------------------------------------------------------------------
tar_option_set(
    packages = c(
        "dplyr",
        "purrr",
        "sf",
        "fs",
        "arrow",
        "ggplot2",
        "lubridate",
        "mgcv",
        "rmarkdown",
        "PrestoGP",
        "readxl",
        "readr",
        "reshape2",
        "tidyr",
        "stringr",
        "knitr",
        "kableExtra"
    ),
    format = "rds",
    memory = "transient"
)

# Source helper functions
purrr::walk(list.files("R", full.names = TRUE, pattern = "\\.R$"), source)

# -----------------------------------------------------------------------------
# Targets
# -----------------------------------------------------------------------------
list(
    # Example vignette – independent of the rest
    tarchetypes::tar_render(
        example_practice,
        "R/prestogp_example.Rmd",
        output_dir = "docs"
    ),

    # 1) Build reference CSVs (single function that returns a list/data.frame)
    tar_target(
        ref_csvs,
        build_ref_csvs(out_dir = "raw_data")
    ),

    # 2) Track the individual reference files produced
    tar_target(
        csn_sites_ref_file,
        ref_csvs$csn_sites_ref,
        format = "file"
    ),
    tar_target(
        improve_sites_ref_file,
        ref_csvs$improve_sites_ref,
        format = "file"
    ),
    tar_target(
        params_ref_file,
        ref_csvs$params_ref,
        format = "file"
    ),

    # 3) Build the cleaned parquet dataset, explicitly depending on ref files
    tar_target(
        spec_parquet_files,
        build_spec_parquet_from_refs(
            csn_sites_ref_csv = csn_sites_ref_file,
            improve_sites_ref_csv = improve_sites_ref_file,
            params_ref_csv = params_ref_file,
            out_dir = "clean_data/parquet_2"
        ),
        format = "file"
    ),

    # 4) Exploratory analysis Rmd that depends on the cleaned parquet
    #    We pass spec_parquet_files as a parameter so targets sees the dependency.
    tarchetypes::tar_render(
        exploratory_analysis,
        "R/exploratory.Rmd",
        output_dir = "docs",
        params = list(
            spec_parquet_files = spec_parquet_files
        )
    )
)
