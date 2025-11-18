build_spec_parquet_from_refs <- function(
    csn_files = c(
        "raw_data/CSN/raw/EPACSN_2010_2014.txt",
        "raw_data/CSN/raw/EPACSN_2015_2019.txt",
        "raw_data/CSN/raw/EPACSN_2020_2024.txt"
    ),
    improve_files = c(
        "raw_data/IMPROVE/raw/IMPAER_2010_2014.txt",
        "raw_data/IMPROVE/raw/IMPAER_2015_2019.txt",
        "raw_data/IMPROVE/raw/IMPAER_2020_2024.txt"
    ),
    csn_sites_ref_csv = "raw_data/csn_sites_ref.csv.gz",
    improve_sites_ref_csv = "raw_data/improve_sites_ref.csv.gz",
    params_ref_csv = "raw_data/params_ref.csv.gz",
    out_dir = "data/parquet"
) {
    requireNamespace("arrow", quietly = TRUE)
    requireNamespace("dplyr", quietly = TRUE)
    requireNamespace("readr", quietly = TRUE)
    requireNamespace("lubridate", quietly = TRUE)
    requireNamespace("fs", quietly = TRUE)
    requireNamespace("tidyr", quietly = TRUE)
    requireNamespace("stringr", quietly = TRUE)

    is_digits <- function(x) grepl("^[0-9]+$", as.character(x))
    pad9 <- function(x) {
        ifelse(
            is_digits(x),
            stringr::str_pad(as.character(x), 9, pad = "0"),
            as.character(x)
        )
    }

    # ---- references ----
    csn_sites_ref <- readr::read_csv(
        csn_sites_ref_csv,
        show_col_types = FALSE
    ) |>
        dplyr::mutate(
            SiteCode = pad9(SiteCode),
            AQS_SiteCode = pad9(AQS_SiteCode),
            Latitude = as.numeric(Latitude),
            Longitude = as.numeric(Longitude)
        )

    improve_sites_ref <- readr::read_csv(
        improve_sites_ref_csv,
        show_col_types = FALSE
    ) |>
        dplyr::mutate(
            SiteCode = pad9(SiteCode),
            Latitude = as.numeric(Latitude),
            Longitude = as.numeric(Longitude)
        )

    params_ref <- readr::read_csv(params_ref_csv, show_col_types = FALSE) |>
        dplyr::mutate(
            ParamCode = as.character(ParamCode),
            AQSParamCode = as.character(AQSParamCode),
            ParamName = as.character(ParamName),
            Units = as.character(Units)
        )

    # unique ref keys
    csn_sites_by_site <- csn_sites_ref |>
        dplyr::filter(!is.na(SiteCode) & SiteCode != "") |>
        dplyr::distinct(SiteCode, .keep_all = TRUE) |>
        dplyr::select(SiteCode, Latitude, Longitude)

    csn_sites_by_aqs <- csn_sites_ref |>
        dplyr::filter(!is.na(AQS_SiteCode) & AQS_SiteCode != "") |>
        dplyr::distinct(AQS_SiteCode, .keep_all = TRUE) |>
        dplyr::transmute(
            SiteCode = AQS_SiteCode,
            Latitude_aqs = Latitude,
            Longitude_aqs = Longitude
        )

    improve_sites_by_site <- improve_sites_ref |>
        dplyr::filter(!is.na(SiteCode) & SiteCode != "") |>
        dplyr::distinct(SiteCode, .keep_all = TRUE) |>
        dplyr::select(SiteCode, Latitude, Longitude)

    params_by_aqs <- params_ref |>
        dplyr::filter(!is.na(AQSParamCode) & AQSParamCode != "") |>
        dplyr::distinct(AQSParamCode, .keep_all = TRUE) |>
        dplyr::select(AQSParamCode, ParamName, Units_ref = Units)

    params_by_imp <- params_ref |>
        dplyr::filter(!is.na(ParamCode) & ParamCode != "") |>
        dplyr::distinct(ParamCode, .keep_all = TRUE) |>
        dplyr::select(ParamCode, ParamName, Units_ref = Units, AQSParamCode)

    # ---- CSN: DISTINCT IN ARROW, THEN collect ----
    csn_raw_tbl <- arrow::open_dataset(
        csn_files,
        format = "csv",
        delimiter = "|"
    ) |>
        dplyr::select(
            SiteCode,
            POC,
            FactDate,
            AQSParamCode,
            Units,
            FactValue,
            MDL
        ) |>
        # drop exact duplicates before shipping to R
        dplyr::distinct() |>
        dplyr::collect() |>
        dplyr::mutate(
            SiteCode = pad9(SiteCode),
            POC = as.character(POC),
            AQSParamCode = as.character(AQSParamCode)
        )

    csn_final <- csn_raw_tbl |>
        dplyr::left_join(csn_sites_by_site, by = "SiteCode") |>
        dplyr::left_join(csn_sites_by_aqs, by = "SiteCode") |>
        dplyr::mutate(
            Latitude = dplyr::coalesce(Latitude, Latitude_aqs),
            Longitude = dplyr::coalesce(Longitude, Longitude_aqs)
        ) |>
        dplyr::select(-tidyr::ends_with("_aqs")) |>
        dplyr::left_join(params_by_aqs, by = "AQSParamCode") |>
        dplyr::transmute(
            SiteCode,
            POC,
            FactDate,
            AQSParamCode,
            ParamName,
            Units = dplyr::coalesce(Units, Units_ref),
            FactValue,
            MDL,
            Longitude,
            Latitude,
            Network = "CSN"
        )

    # ---- IMPROVE: DISTINCT IN ARROW, THEN collect ----
    improve_raw_tbl <- arrow::open_dataset(
        improve_files,
        format = "csv",
        delimiter = "|"
    ) |>
        dplyr::select(
            SiteCode,
            POC,
            FactDate,
            ParamCode,
            Units,
            FactValue,
            MDL
        ) |>
        # drop exact duplicates before shipping to R
        dplyr::distinct() |>
        dplyr::collect() |>
        dplyr::mutate(
            SiteCode = pad9(SiteCode), # harmless for alphanumerics
            POC = as.character(POC),
            ParamCode = as.character(ParamCode)
        )

    improve_final <- improve_raw_tbl |>
        dplyr::left_join(improve_sites_by_site, by = "SiteCode") |>
        dplyr::left_join(params_by_imp, by = "ParamCode") |>
        dplyr::transmute(
            SiteCode,
            POC,
            FactDate,
            AQSParamCode,
            ParamName,
            Units = dplyr::coalesce(Units, Units_ref),
            FactValue,
            MDL,
            Longitude,
            Latitude,
            Network = "IMPROVE"
        )

    # ---- combine, add Year, write parquet ----
    spec_data <- dplyr::bind_rows(csn_final, improve_final) |>
        dplyr::mutate(Year = lubridate::year(FactDate))

    fs::dir_create(out_dir)
    arrow::write_dataset(
        arrow::as_arrow_table(spec_data),
        out_dir,
        format = "parquet",
        partitioning = c("AQSParamCode", "Year")
    )

    fs::dir_ls(out_dir, recurse = TRUE, type = "file")
}
