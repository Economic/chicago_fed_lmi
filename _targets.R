# Load packages
source("packages.R")

current_timestamp = Sys.time()

# Define the pipeline using tar_assign()
tar_assign({
  # Download BLS unemployment rate data
  bls_data_raw <- tar_target(
    {
      current_timestamp

      get_bls(
        series = "LNS14000000",
        start = 2008,
        end = as.numeric(format(Sys.Date(), "%Y"))
      )
    }
  )

  # Process BLS data
  bls_data <- tar_target(
    bls_data_raw %>%
      mutate(
        unemployment_rate = as.numeric(value),
        source = "BLS"
      ) %>%
      select(date, unemployment_rate, source) %>%
      arrange(date) |>
      mutate(date = ym(paste(year(date), month(date))))
  )

  # Download Chicago Fed FCR data
  fcr_file <- tar_target(
    {
      current_timestamp

      dir.create("data", showWarnings = FALSE)
      download.file(
        url = "https://www.chicagofed.org/-/media/publications/chicago-fed-labor-market-indicators/chi-labor-market-indicators.xlsx",
        destfile = "data/chi-labor-market-indicators.xlsx",
        mode = "wb"
      )
      "data/chi-labor-market-indicators.xlsx"
    },
    format = "file"
  )

  # Process FCR data
  fcr_data <- tar_target({
    # Read the Excel file
    raw_data <- read_excel(fcr_file, sheet = "1. Rates")

    # Find the FCR column (Flow-consistent Unemployment Rate)
    # and process the data
    raw_data %>%
      select(date, fcr) %>%
      filter(!is.na(date), !is.na(fcr)) %>%
      mutate(
        date = as.Date(date),
        unemployment_rate = as.numeric(fcr),
        source = "Chicago Fed FCR"
      ) %>%
      select(date, unemployment_rate, source) %>%
      arrange(date) |>
      mutate(date = ym(paste(year(date), month(date))))
  })

  # Combine both datasets (long format for plotting)
  combined_data <- tar_target(
    bind_rows(bls_data, fcr_data) %>%
      arrange(date, source)
  )

  # Create wide format data for CSV export
  combined_data_wide <- tar_target(
    combined_data %>%
      pivot_wider(
        names_from = source,
        values_from = unemployment_rate
      ) %>%
      rename(
        Date = date,
        `BLS unemployment rate` = BLS,
        `Chicago Fed FCR` = `Chicago Fed FCR`
      )
  )

  # Save combined data as CSV
  combined_data_csv <- tar_target(
    {
      dir.create("docs", showWarnings = FALSE)
      write_csv(combined_data_wide, "docs/data.csv")
      "docs/data.csv"
    },
    format = "file"
  )

  # Render Quarto site
  site <- tar_quarto(
    path = "index.qmd"
  )
})
