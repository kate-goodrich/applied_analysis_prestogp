build_ref_csvs <- function(
    csn_sites_xlsx = "/ddn/gs1/group/set/PrestoGP_PM/raw_data/CSN/kylemessier_20250925_144631_tOOty.xlsx",
    improve_sites_xlsx = "/ddn/gs1/group/set/PrestoGP_PM/raw_data/IMPROVE/kylemessier_20250925_144547_1KxOK.xlsx",
    params_xlsx = "/ddn/gs1/group/set/PrestoGP_PM/raw_data/CSN/kylemessier_20250925_214242_0Qus1.xlsx",
    out_dir = "raw_data"
) {
    requireNamespace("readxl", quietly = TRUE)
    requireNamespace("dplyr", quietly = TRUE)
    requireNamespace("stringr", quietly = TRUE)
    requireNamespace("lubridate", quietly = TRUE)
    requireNamespace("readr", quietly = TRUE)
    requireNamespace("fs", quietly = TRUE)

    parse_best_dates <- function(df) {
        as_dt <- function(x) suppressWarnings(lubridate::mdy(x))
        df |>
            dplyr::mutate(
                DataEndDate = as_dt(.data[["DataEndDate"]]),
                EndDate = as_dt(.data[["EndDate"]]),
                DataStartDate = as_dt(.data[["DataStartDate"]]),
                StartDate = as_dt(.data[["StartDate"]]),
                .recency_key = dplyr::coalesce(
                    .data$DataEndDate,
                    .data$EndDate,
                    .data$DataStartDate,
                    .data$StartDate
                )
            )
    }

    clean_sites_min <- function(path_xlsx) {
        readxl::read_excel(path_xlsx) |>
            dplyr::select(
                "SiteID",
                "SiteCode",
                "SiteName",
                "Country",
                "State",
                "County",
                "AQSCode",
                "Latitude",
                "Longitude",
                "StartDate",
                "EndDate",
                "DataStartDate",
                "DataEndDate"
            ) |>
            dplyr::mutate(
                SiteID = as.character(.data$SiteID),
                SiteCode = as.character(.data$SiteCode),
                SiteName = stringr::str_squish(as.character(.data$SiteName)),
                Country = as.character(.data$Country),
                State = as.character(.data$State),
                County = as.character(.data$County),
                AQS_SiteCode = as.character(.data$AQSCode),
                Latitude = suppressWarnings(as.numeric(.data$Latitude)),
                Longitude = suppressWarnings(as.numeric(.data$Longitude))
            ) |>
            parse_best_dates() |>
            dplyr::filter(!is.na(.data$SiteCode) & .data$SiteCode != "") |>
            dplyr::arrange(
                .data$SiteCode,
                dplyr::desc(.data$.recency_key),
                dplyr::desc(.data$StartDate)
            ) |>
            dplyr::distinct(.data$SiteCode, .keep_all = TRUE) |>
            dplyr::select(
                .data$SiteID,
                .data$SiteCode,
                .data$SiteName,
                .data$State,
                .data$County,
                .data$Latitude,
                .data$Longitude,
                .data$AQS_SiteCode
            )
    }

    clean_params_min <- function(path_xlsx) {
        readxl::read_excel(path_xlsx) |>
            dplyr::transmute(
                ParamID = as.character(.data$ParamID),
                ParamCode = as.character(.data$ParamCode),
                ParamName = stringr::str_squish(as.character(.data$ParamName)),
                Units = as.character(.data$Units),
                AQSParamCode = as.character(.data$AQSCode)
            ) |>
            dplyr::filter(
                !is.na(.data$ParamName) &
                    ((!is.na(.data$ParamCode) & .data$ParamCode != "") |
                        (!is.na(.data$AQSParamCode) & .data$AQSParamCode != ""))
            ) |>
            dplyr::distinct(
                .data$ParamCode,
                .data$AQSParamCode,
                .keep_all = TRUE
            )
    }

    fs::dir_create(out_dir)

    csn_sites_ref <- clean_sites_min(csn_sites_xlsx)
    improve_sites_ref <- clean_sites_min(improve_sites_xlsx)
    params_ref <- clean_params_min(params_xlsx)

    f_out_csn <- fs::path(out_dir, "csn_sites_ref.csv.gz")
    f_out_imp <- fs::path(out_dir, "improve_sites_ref.csv.gz")
    f_out_params <- fs::path(out_dir, "params_ref.csv.gz")

    readr::write_csv(csn_sites_ref, f_out_csn, na = "", quote = "needed")
    readr::write_csv(improve_sites_ref, f_out_imp, na = "", quote = "needed")
    readr::write_csv(params_ref, f_out_params, na = "", quote = "needed")

    list(
        csn_sites_ref = f_out_csn,
        improve_sites_ref = f_out_imp,
        params_ref = f_out_params
    )
}
